use std::cmp::PartialOrd;
use std::fmt::Display;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;

// Accepts D: PartialOrd for convenience; will panic if not Ord.
// Will sort `data`, hence the `&mut`.
pub fn build<D>(data: &mut Vec<(D, f64)>, step: f64) -> Vec<(D, f64)>
where
    D: Copy + Default + PartialOrd,
{
    if data.is_empty() {
        return vec![(Default::default(), 0.0), (Default::default(), 1.0)];
    }

    data.sort_by(|t1, t2| t1.0.partial_cmp(&t2.0).unwrap());

    let mut cdf: Vec<(D, f64)> = Vec::with_capacity(data.len());
    let mut dcurr = data[0].0;
    let mut wtotal = 0.0;
    for &(d, w) in data.iter() {
        if d != dcurr {
            cdf.push((dcurr, wtotal));
            dcurr = d;
        }
        wtotal += w;
    }
    cdf.push((dcurr, wtotal));

    let mut result: Vec<(D, f64)> = Vec::with_capacity((1.0 / step).ceil() as usize);
    result.push((cdf[0].0, 0.0));

    let mut height: f64 = step;
    for (d, w) in cdf {
        let h = w / wtotal;
        if h > height {
            result.push((d, h));
            height = ((h / step).floor() + 1.0) * step;
        }
    }
    result
}

pub fn dump<D, P>(cdf: &[(D, f64)], path: P) -> Result<(), io::Error>
where
    D: Copy + Display,
    P: AsRef<Path>,
{
    let file =
        fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path)?;
    let mut bw = io::BufWriter::new(file);
    for &(x, y) in cdf.iter() {
        writeln!(bw, "{} {}", x, y)?;
    }
    Ok(())
}
