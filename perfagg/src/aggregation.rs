extern crate ipnet;
extern crate treebitmap;

use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr};

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use treebitmap::IpLookupTable;

/// Aggregate a set of prefixes into a set of less specific prefixes.
///
/// `aggregate_prefixes` calls `can_aggregate` on each pair of sibling prefixes to check if
/// they can be aggregated or not. There is no guarantee on the order in which prefixes will be
/// passed to `can_aggregate`.
///
/// # Examples
///
/// ```
/// input: HashSet<IpNet> = ["10.0.6.0/24", "10.0.7.0/24"]
///     .iter()
///     .map(|e| e.parse::<IpNet>().unwrap())
///     .collect();
/// output: HashSet<IpNet> = ["10.0.4.0/22"]
///     .iter()
///     .map(|e| e.parse::<IpNet>().unwrap())
///     .collect();
/// let slash22s: &dyn Fn(&IpNet, &IpNet) -> bool =
///         &|n1, n2| n1.prefix_len() > 22 && n2.prefix_len() > 22
/// assert_eq!(aggregate_prefixes(&input, slash22s), output);
/// ```
pub fn aggregate_prefixes(
    start_prefixes: &HashSet<IpNet>,
    mut can_aggregate: impl FnMut(&IpNet, &IpNet) -> bool,
) -> HashSet<IpNet> {
    let mut aggregated: HashSet<IpNet> = HashSet::new();
    let mut aggregated_supernets: HashSet<IpNet> = HashSet::new();
    let mut next_aggregated_supernets: HashSet<IpNet> = HashSet::new();
    let mut prefixes: HashSet<IpNet> = start_prefixes.clone();
    let mut next_prefixes: HashSet<IpNet> = HashSet::new();

    // Iterate until all prefixes are aggregated
    while !prefixes.is_empty() {
        // Iterate to aggregate all prefixes at the current prefix length
        while !prefixes.is_empty() {
            let prefix: IpNet = *prefixes.iter().next().unwrap();
            let parent: IpNet = prefix.supernet().unwrap();
            let sibling: IpNet =
                parent.subnets(prefix.prefix_len()).unwrap().find(|p| p != &prefix).unwrap();
            debug_assert!(prefix.is_sibling(&sibling));
            prefixes.remove(&prefix);
            if aggregated_supernets.contains(&sibling) {
                // The sibling prefix overlaps previously-deaggregated prefixes; stop aggregation
                aggregated.insert(prefix);
                next_aggregated_supernets.insert(parent);
            } else if can_aggregate(&prefix, &sibling) {
                next_prefixes.insert(parent);
            } else {
                aggregated.insert(prefix);
                next_aggregated_supernets.insert(parent);
                if prefixes.contains(&sibling) {
                    prefixes.remove(&sibling);
                    aggregated.insert(sibling);
                }
            }
            debug_assert!(
                next_aggregated_supernets.contains(&parent) || next_prefixes.contains(&parent),
                "Parent supernet needed in next iteration to check stopping conditions."
            );
        }
        next_aggregated_supernets.extend(aggregated_supernets.iter().filter_map(|p| p.supernet()));
        aggregated_supernets = next_aggregated_supernets;
        prefixes = next_prefixes;
        next_aggregated_supernets = HashSet::new();
        next_prefixes = HashSet::new();
    }
    aggregated
}

fn noncovered_insert<T>(addr: T, pfxlen: u32, trie: &mut IpLookupTable<T, bool>)
where
    T: treebitmap::address::Address,
{
    match trie.longest_match(addr) {
        None => {
            trie.insert(addr, pfxlen, true);
        }
        Some((prev, len, _)) => {
            debug_assert!(pfxlen != len);
            if len > pfxlen {
                return;
            }
            trie.remove(prev, len);
            debug_assert!(trie.longest_match(addr).is_none());
            trie.insert(addr, pfxlen, true);
        }
    };
}

pub fn noncovered_prefixes<'a, I>(prefixes: I) -> HashSet<IpNet>
where
    I: Iterator<Item = &'a IpNet>,
{
    let mut trie4: IpLookupTable<Ipv4Addr, bool> = IpLookupTable::new();
    let mut trie6: IpLookupTable<Ipv6Addr, bool> = IpLookupTable::new();
    for prefix in prefixes {
        match prefix {
            IpNet::V4(v4net) => {
                noncovered_insert(v4net.addr(), u32::from(v4net.prefix_len()), &mut trie4)
            }
            IpNet::V6(v6net) => {
                noncovered_insert(v6net.addr(), u32::from(v6net.prefix_len()), &mut trie6)
            }
        }
    }
    let mut noncovered: HashSet<IpNet> = trie4
        .iter()
        .map(|(addr, len, _)| IpNet::from(Ipv4Net::new(addr, len as u8).unwrap()))
        .collect();
    noncovered.extend(
        trie6.iter().map(|(addr, len, _)| IpNet::from(Ipv6Net::new(addr, len as u8).unwrap())),
    );
    noncovered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mkset(prefixes: &[&str]) -> HashSet<IpNet> {
        prefixes.iter().map(|e| e.parse::<IpNet>().unwrap()).collect()
    }

    #[test]
    fn agg_full_slash22s() {
        let agg_slash22s = &|n1: &IpNet, n2: &IpNet| n1.prefix_len() > 22 && n2.prefix_len() > 22;

        let mut input: HashSet<IpNet>;
        let output: HashSet<IpNet> = mkset(&["10.0.0.0/22", "10.0.4.0/22"]);

        input = mkset(&["10.0.0.0/24", "10.0.3.0/24", "10.0.4.0/24", "10.0.6.0/24"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);

        input = mkset(&["10.0.0.0/24", "10.0.7.0/24"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);

        input = mkset(&["10.0.0.0/23", "10.0.6.0/23"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);

        input = mkset(&["10.0.0.0/22", "10.0.4.0/22"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);
    }

    #[test]
    fn agg_partial_slash22s() {
        let agg_slash22s = &|n1: &IpNet, n2: &IpNet| n1.prefix_len() > 22 && n2.prefix_len() > 22;

        let mut input: HashSet<IpNet>;
        let mut output: HashSet<IpNet>;

        input = mkset(&["10.0.0.0/24"]);
        output = mkset(&["10.0.0.0/22"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);

        input = mkset(&["10.0.3.0/24"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);

        input = mkset(&["10.0.6.0/24"]);
        output = mkset(&["10.0.4.0/22"]);
        assert_eq!(aggregate_prefixes(&input, agg_slash22s), output);
    }

    #[test]
    fn agg_gaps() {
        let mut input: HashSet<IpNet>;
        let mut output: HashSet<IpNet>;
        let mut blacklist: HashSet<IpNet>;

        blacklist = mkset(&["10.0.2.0/24", "10.0.3.0/24"]);
        let agg_gaps = &|n1: &IpNet, n2: &IpNet| {
            if blacklist.contains(n1) || blacklist.contains(n2) {
                false
            } else {
                n1.prefix_len() > 22 && n2.prefix_len() > 22
            }
        };
        input = mkset(&["10.0.0.0/24", "10.0.2.0/24", "10.0.3.0/24", "10.0.4.0/24"]);
        output = mkset(&["10.0.0.0/23", "10.0.2.0/24", "10.0.3.0/24", "10.0.4.0/22"]);
        assert_eq!(aggregate_prefixes(&input, agg_gaps), output);

        input = mkset(&["10.0.0.0/24", "10.0.4.0/24"]);
        output = mkset(&["10.0.0.0/22", "10.0.4.0/22"]);
        assert_eq!(aggregate_prefixes(&input, agg_gaps), output);

        blacklist = mkset(&["10.0.2.0/23"]);
        let agg_gaps = &|n1: &IpNet, n2: &IpNet| {
            if blacklist.contains(n1) || blacklist.contains(n2) {
                false
            } else {
                n1.prefix_len() > 22 && n2.prefix_len() > 22
            }
        };
        input = mkset(&["10.0.0.0/24", "10.0.4.0/24"]);
        output = mkset(&["10.0.0.0/23", "10.0.4.0/22"]);
        assert_eq!(aggregate_prefixes(&input, agg_gaps), output);

        blacklist = mkset(&["10.0.2.0/24", "10.0.6.0/23"]);
        let agg_gaps = &|n1: &IpNet, n2: &IpNet| {
            if blacklist.contains(n1) || blacklist.contains(n2) {
                false
            } else {
                n1.prefix_len() > 22 && n2.prefix_len() > 22
            }
        };
        input = mkset(&["10.0.0.0/24", "10.0.3.0/24", "10.0.4.0/24", "10.0.7.0/24"]);
        output = mkset(&["10.0.0.0/23", "10.0.3.0/24", "10.0.4.0/23", "10.0.6.0/23"]);
        assert_eq!(aggregate_prefixes(&input, agg_gaps), output);
    }

    #[test]
    fn agg_merge() {
        let mut input: HashSet<IpNet>;
        let mut output: HashSet<IpNet>;
        let mut blacklist: HashSet<IpNet>;

        blacklist = mkset(&["10.0.2.0/24", "10.0.3.0/24"]);
        let agg_merge = &|n1: &IpNet, n2: &IpNet| {
            if blacklist.contains(n1) || blacklist.contains(n2) {
                false
            } else {
                n1.prefix_len() > 20 && n2.prefix_len() > 20
            }
        };
        input = mkset(&["10.0.0.0/24", "10.0.2.0/24", "10.0.3.0/24", "10.0.8.0/24"]);
        output = mkset(&["10.0.0.0/23", "10.0.2.0/24", "10.0.3.0/24", "10.0.8.0/21"]);
        assert_eq!(aggregate_prefixes(&input, agg_merge), output);

        blacklist = mkset(&["10.0.2.0/24", "10.0.3.0/24"]);
        let agg_merge = &|n1: &IpNet, n2: &IpNet| {
            if blacklist.contains(n1) || blacklist.contains(n2) {
                false
            } else {
                n1.prefix_len() > 18 && n2.prefix_len() > 18
            }
        };
        input = mkset(&["10.0.0.0/24", "10.0.2.0/24", "10.0.3.0/24", "10.0.8.0/24"]);
        output = mkset(&["10.0.0.0/23", "10.0.2.0/24", "10.0.3.0/24", "10.0.8.0/21"]);
        assert_eq!(aggregate_prefixes(&input, agg_merge), output);

        input =
            mkset(&["10.0.0.0/24", "10.0.2.0/24", "10.0.3.0/24", "10.0.8.0/24", "10.0.16.0/24"]);
        output =
            mkset(&["10.0.0.0/23", "10.0.2.0/24", "10.0.3.0/24", "10.0.8.0/21", "10.0.16.0/20"]);
        assert_eq!(aggregate_prefixes(&input, agg_merge), output);
    }

    #[test]
    fn cov_same_len() {
        let input: HashSet<IpNet> = mkset(&[
            "10.0.0.0/24",
            "10.0.2.0/24",
            "10.0.3.0/24",
            "10.0.0.0/23",
            "10.0.2.0/23",
            "10.0.0.0/22",
        ]);
        let output: HashSet<IpNet> = mkset(&["10.0.0.0/24", "10.0.2.0/24", "10.0.3.0/24"]);;
        assert_eq!(noncovered_prefixes(input.iter()), output);
    }

    #[test]
    fn cov_diff_len() {
        let mut input: HashSet<IpNet> = mkset(&[
            "10.0.0.0/21",
            "10.0.0.0/22",
            "10.0.0.0/23",
            "10.0.0.0/24",
            "10.0.2.0/23",
            "10.0.2.0/24",
            "10.0.3.0/24",
            "10.0.4.0/22",
            "10.0.12.0/22",
        ]);
        let mut output: HashSet<IpNet> =
            mkset(&["10.0.0.0/24", "10.0.2.0/24", "10.0.3.0/24", "10.0.4.0/22", "10.0.12.0/22"]);
        assert_eq!(noncovered_prefixes(input.iter()), output);

        input = mkset(&[
            "10.0.0.0/21",
            "10.0.0.0/22",
            "10.0.0.0/23",
            "10.0.0.0/24",
            "10.0.2.0/23",
            "10.0.2.0/24",
            "10.0.3.0/24",
            "10.0.4.0/22",
            "10.0.5.0/24",
            "10.0.6.0/23",
            "10.0.12.0/22",
        ]);
        output = mkset(&[
            "10.0.0.0/24",
            "10.0.2.0/24",
            "10.0.3.0/24",
            "10.0.5.0/24",
            "10.0.6.0/23",
            "10.0.12.0/22",
        ]);
        assert_eq!(noncovered_prefixes(input.iter()), output);
    }
}
