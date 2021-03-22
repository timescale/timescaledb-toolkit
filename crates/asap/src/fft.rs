use std::f64::consts::PI;

/* 
 * Computes the discrete Fourier transform (DFT) of the given complex vector, storing the result back into the vector.
 * The vector can have any length. This is a wrapper function.
 */
pub fn transform(real: &mut Vec<f64>, imag: &mut Vec<f64>) {
    assert_eq!(real.len(), imag.len());

    let n = real.len();
    if n == 0 {
         return;
    }
    else if n & (n-1) == 0 { // Is power of 2
        transform_radix2(real, imag);
    }
    else  { // More complicated algorithm for arbitrary sizes
        transform_bluestein(real, imag);
    }
}


/* 
 * Computes the inverse discrete Fourier transform (IDFT) of the given complex vector, storing the result back into the vector.
 * The vector can have any length. This is a wrapper function. This transform does not perform scaling, so the inverse is not a true inverse.
 */
pub fn inverse_transform(real: &mut Vec<f64>, imag: &mut Vec<f64>) {
    transform(imag, real);
}


/* 
 * Computes the discrete Fourier transform (DFT) of the given complex vector, storing the result back into the vector.
 * The vector's length must be a power of 2. Uses the Cooley-Tukey decimation-in-time radix-2 algorithm.
 */
fn transform_radix2(real: &mut Vec<f64>, imag: &mut Vec<f64>) {
    // Initialization
    let n = real.len();
    if n == 1 {  // Trivial transform
        return;
    }
    let mut levels = 100;
    for i in 0..32 {
        if 1 << i == n {
            levels = i;  // Equal to log2(n)
        }
    }
    debug_assert!(levels < 32);

    let mut cos_table = vec![0.0; n / 2];
    let mut sin_table = vec![0.0; n / 2];
    for i in 0..n/2 {
        cos_table[i] = (2.0 * PI * i as f64 / n as f64).cos();
        sin_table[i] = (2.0 * PI * i as f64 / n as f64).sin();
    }

    // Bit-reversed addressing permutation
    for i in 0..n {
        let j = reverse_bits(i as u32, levels) as usize;
        if j > i {
            let mut temp = real[i];
            real[i] = real[j];
            real[j] = temp;
            temp = imag[i];
            imag[i] = imag[j];
            imag[j] = temp;
        }
    }

    // Cooley-Tukey decimation-in-time radix-2 FFT
    let mut size = 2;
    while size <= n {
        let halfsize = size / 2;
        let tablestep = n / size;
        for i in (0..n).step_by(size) {
            let mut j = i;
            let mut k = 0;
            while  j < i + halfsize {
                let tpre =  real[j+halfsize] * cos_table[k] + imag[j+halfsize] * sin_table[k];
                let tpim = -real[j+halfsize] * sin_table[k] + imag[j+halfsize] * cos_table[k];
                real[j + halfsize] = real[j] - tpre;
                imag[j + halfsize] = imag[j] - tpim;
                real[j] += tpre;
                imag[j] += tpim;
                j += 1;
                k += tablestep;
            }
        }
        size *= 2;
    }

    // Returns the integer whose value is the reverse of the lowest 'bits' bits of the integer 'x'.
    fn reverse_bits(x: u32, bits: u32) -> u32 {
        let mut x = x;
        let mut y = 0;
        for _ in 0..bits {
            y = (y << 1) | (x & 1);
            x >>= 1;
        }
        return y;
    }
}

/*
 * Computes the discrete Fourier transform (DFT) of the given complex vector, storing the result back into the vector.
 * The vector can have any length. This requires the convolution function, which in turn requires the radix-2 FFT function.
 * Uses Bluestein's chirp z-transform algorithm.
 */
fn transform_bluestein(real: &mut Vec<f64>, imag: &mut Vec<f64>) {
    // Find a power-of-2 convolution length m such that m >= n * 2 + 1
    let n = real.len();
    let mut m = 1;
    while m < n * 2 + 1 {
        m *= 2;
    }

    // Trignometric tables
    let mut cos_table = vec![0.0; n];
    let mut sin_table = vec![0.0; n];
    for i in 0..n {
        let j = (i * i % (n * 2)) as f64;  // This is more accurate than j = i * i
        cos_table[i] = (PI * j / n as f64).cos();
        sin_table[i] = (PI * j / n as f64).sin();
    }

    // Temporary vectors and preprocessing
    let mut areal = vec![0.0; m];
    let mut aimag = vec![0.0; m];
    for i in 0..n {
        areal[i] =  real[i] * cos_table[i] + imag[i] * sin_table[i];
        aimag[i] = -real[i] * sin_table[i] + imag[i] * cos_table[i];
    }
    for i in n..m {
        areal[i] = 0.0;
        aimag[i] = 0.0;
    }

    let mut breal = vec![0.0; m];
    let mut bimag = vec![0.0; m];
    breal[0] = cos_table[0];
    bimag[0] = sin_table[0];
    for i in 1..n {
        breal[i] = cos_table[i];
        breal[m - i] = cos_table[i];
        bimag[i] = sin_table[i];
        bimag[m - i] = sin_table[i];
    }
    for i in n..=(m-n) {
        breal[i] = 0.0;
        bimag[i] = 0.0;
    }

    // Convolution
    let mut creal = vec![0.0; m];
    let mut cimag = vec![0.0; m];
    convolve_complex(&mut areal, &mut aimag, &mut breal, &mut bimag, &mut creal, &mut cimag);

    // Postprocessing
    for i in 0..n {
        real[i] =  creal[i] * cos_table[i] + cimag[i] * sin_table[i];
        imag[i] = -creal[i] * sin_table[i] + cimag[i] * cos_table[i];
    }
}


// /*
//  * Computes the circular convolution of the given real vectors. Each vector's length must be the same.
//  */
// function convolveReal(x, y, out) {
//     if (x.length != y.length || x.length != out.length)
//         throw "Mismatched lengths";
//     var zeros = new Array(x.length);
//     for (var i = 0; i < zeros.length; i++)
//         zeros[i] = 0;
//     convolve_complex(x, zeros, y, zeros.slice(), out, zeros.slice());
// }


// /*
//  * Computes the circular convolution of the given complex vectors. Each vector's length must be the same.
//  */
fn convolve_complex(xreal: &mut Vec<f64>, ximag: &mut Vec<f64>, yreal: &mut Vec<f64>, yimag: &mut Vec<f64>, outreal: &mut Vec<f64>, outimag: &mut Vec<f64>) {
    let n = xreal.len();

    transform(xreal, ximag);
    transform(yreal, yimag);
    for i in 0..n {
        let temp = xreal[i] * yreal[i] - ximag[i] * yimag[i];
        ximag[i] = ximag[i] * yreal[i] + xreal[i] * yimag[i];
        xreal[i] = temp;
    }
    inverse_transform(xreal, ximag);
    for i in 0..n  {  // Scaling (because this FFT implementation omits it)
        outreal[i] = xreal[i] / n as f64;
        outimag[i] = ximag[i] / n as f64;
    }
}