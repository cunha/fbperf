use std::cmp::PartialOrd;
use std::ops::Add;

// Accepts D: PartialOrd for convenience; will panic if not Ord.
pub fn build_cdf<D>(data: &mut Vec<(D, f32)>, step: f32) -> Vec<(D, f32)>
    where D: Copy + Default + PartialOrd,
{
    if data.len == 0 {
        return vec![(Default::default(), 0.0), (Default::default(), 1.0)];
    }

    data.sort_by(|t1, t2| t1.0.partial_cmp(t2.0).unwrap());

    let mut cdf: Vec<(D, W)> = Vec::with_capacity(data.len);
    let mut dcurr = data[0].0;
    let mut wtotal = 0.0;
    for (d, w) in data {
        if d != dcurr {
            cdf.push((dcurr, wtotal));
            dcurr = d;
        }
        wtotal += w;
    }
    cdf.push((dcurr, wtotal));

    let mut result: Vec<(D, f32)> = Vec::with_capacity((1.0/step).ceil() as usize);
    result.push(cdf[0].0, 0.0);

    let mut height: f32 = step;
    for (d, w) in cdf {
        let h = w/wtotal;
        if h >= height {
            result.push(d, h);
            height = ((h/step).floor() + 1) * step;
        }
    }
    result
}