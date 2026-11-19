//! Go `math` functions with Go's results bit for bit (Go 1.26 on amd64, as the upstream release binaries run).
//! Rust's `f64` methods call the platform libm, which differs from Go in the last bit for many inputs.
//!
//! `exp` and `log` port the amd64 assembly (`exp_amd64.s` with the FMA path that CPUs with AVX and FMA take,
//! and `log_amd64.s`); the others port the pure Go code (`sin.go`, `tan.go`, `trig_reduce.go`, `atan.go`,
//! `asin.go`, `sinh.go`, `tanh.go`, `asinh.go`, `acosh.go`, `atanh.go`, `log1p.go`, `log10.go`, `frexp.go`,
//! `exp.go`, `ldexp.go`).

#![allow(
    clippy::excessive_precision,
    clippy::approx_constant,
    clippy::neg_cmp_op_on_partial_ord,
    clippy::manual_range_contains
)]

use std::f64::consts::{FRAC_PI_2, FRAC_PI_4, LN_2, LOG10_E, LOG2_E};

const SHIFT: u32 = 52;
const MASK: u64 = 0x7FF;
const BIAS: i64 = 1023;

/// Go `math.Exp` (amd64 assembly, FMA path).
pub fn exp(x: f64) -> f64 {
    const LN2U: f64 = 0.69314718055966295651160180568695068359375; // Upper half LN2.
    const LN2L: f64 = 0.28235290563031577122588448175013436025525412068e-12; // Lower half LN2.
    const OVERFLOW: f64 = 7.09782712893384e+02;
    const POS_INF: u64 = 0x7FF0000000000000;
    const NEG_INF: u64 = 0xFFF0000000000000;
    const D: [f64; 9] = [
        0.5,
        1.0,
        2.0,
        1.6666666666666666667e-1,
        4.1666666666666666667e-2,
        8.3333333333333333333e-3,
        1.3888888888888888889e-3,
        1.9841269841269841270e-4,
        2.4801587301587301587e-5,
    ];
    let bits = x.to_bits();
    if (bits & !(1u64 << 63)) as i64 >= POS_INF as i64 {
        // Not finite: -Inf gives 0, NaN and +Inf give x.
        if bits == NEG_INF {
            return 0.0;
        }
        return x;
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }
    // CVTSD2SL: round to nearest even; out of range gives the "integer indefinite" value.
    let t = LOG2_E * x;
    let mut k: i32 = if t >= -2147483648.0 && t < 2147483648.0 { t.round_ties_even() as i32 } else { i32::MIN };
    let kf = k as f64;
    let mut x0 = (-kf).mul_add(LN2U, x);
    x0 = (-kf).mul_add(LN2L, x0);
    // Reduce the argument.
    x0 *= 0.0625;
    // Taylor series evaluation.
    let mut x1 = D[8];
    x1 = x1.mul_add(x0, D[7]);
    x1 = x1.mul_add(x0, D[6]);
    x1 = x1.mul_add(x0, D[5]);
    x1 = x1.mul_add(x0, D[4]);
    x1 = x1.mul_add(x0, D[3]);
    x1 = x1.mul_add(x0, D[0]);
    x1 = x1.mul_add(x0, D[1]);
    x0 *= x1;
    for _ in 0..3 {
        x1 = x0 + D[2];
        x0 *= x1;
    }
    x1 = x0 + D[2];
    x0 = x0.mul_add(x1, D[1]);
    // Return fr * 2**exponent.
    k = k.wrapping_add(0x3FF);
    if k <= 0 {
        // Denormal.
        if k < -52 {
            return 0.0;
        }
        k = k.wrapping_add(0x3FE);
        x0 *= f64::from_bits((k as u32 as u64) << 52);
        k = 1;
    } else if k >= 0x7FF {
        return f64::INFINITY;
    }
    x0 * f64::from_bits((k as u32 as u64) << 52)
}

/// Go `math.Log` (amd64 assembly).
pub fn log(x: f64) -> f64 {
    const HSQRT2: f64 = 7.07106781186547524401e-01;
    const LN2HI: f64 = 6.93147180369123816490e-01;
    const LN2LO: f64 = 1.90821492927058770002e-10;
    const L1: f64 = 6.666666666666735130e-01;
    const L2: f64 = 3.999999999940941908e-01;
    const L3: f64 = 2.857142874366239149e-01;
    const L4: f64 = 2.222219843214978396e-01;
    const L5: f64 = 1.818357216161805012e-01;
    const L6: f64 = 1.531383769920937332e-01;
    const L7: f64 = 1.479819860511658591e-01;
    let bits = x.to_bits();
    if bits & !(1u64 << 63) == 0 {
        return f64::NEG_INFINITY;
    }
    if (bits as i64) < 0 {
        return f64::from_bits(0x7FF8000000000001);
    }
    if bits >= 0x7FF0000000000000 {
        return x;
    }
    // f1, ki := math.Frexp(x); k := float64(ki)
    let mut f1 = f64::from_bits((bits & 0x000FFFFFFFFFFFFF) | 0x3FE0000000000000);
    let mut k = (((bits >> 52) & 0x7FF) as i32 - 0x3FE) as f64;
    // if !(Sqrt2/2 < f1) { k -= 1; f1 *= 2 }
    if !(HSQRT2 < f1) {
        k -= 1.0;
        f1 *= 2.0;
    }
    let f = f1 - 1.0;
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let t1 = s2 * (((L7 * s4 + L5) * s4 + L3) * s4 + L1);
    let t2 = s4 * ((L6 * s4 + L4) * s4 + L2);
    let r = t1 + t2;
    let hfsq = 0.5 * f * f;
    k * LN2HI - ((hfsq - (s * (hfsq + r) + k * LN2LO)) - f)
}

/// Go `normalize`.
#[inline]
fn normalize(x: f64) -> (f64, i64) {
    const SMALLEST_NORMAL: f64 = 2.2250738585072014e-308;
    if x.abs() < SMALLEST_NORMAL {
        return (x * (1u64 << 52) as f64, -52);
    }
    (x, 0)
}

/// Go `math.Frexp`.
pub fn frexp(f: f64) -> (f64, i64) {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return (f, 0);
    }
    let (f, mut exp) = normalize(f);
    let mut x = f.to_bits();
    exp += ((x >> SHIFT) & MASK) as i64 - BIAS + 1;
    x &= !(MASK << SHIFT);
    x |= ((-1 + BIAS) as u64) << SHIFT;
    (f64::from_bits(x), exp)
}

/// Go `math.Ldexp` (pure Go on amd64).
pub fn ldexp(frac: f64, exp: i64) -> f64 {
    if frac == 0.0 || frac.is_infinite() || frac.is_nan() {
        return frac;
    }
    let (frac, e) = normalize(frac);
    let mut exp = exp + e;
    let mut x = frac.to_bits();
    exp += ((x >> SHIFT) & MASK) as i64 - BIAS;
    if exp < -1075 {
        return 0.0f64.copysign(frac); // Underflow.
    }
    if exp > 1023 {
        // Overflow.
        return if frac < 0.0 { f64::NEG_INFINITY } else { f64::INFINITY };
    }
    let mut m = 1.0;
    if exp < -1022 {
        // Denormal.
        exp += 53;
        m = 1.0 / (1u64 << 53) as f64;
    }
    x &= !(MASK << SHIFT);
    x |= ((exp + BIAS) as u64) << SHIFT;
    m * f64::from_bits(x)
}

/// Go `math.Log2`.
pub fn log2(x: f64) -> f64 {
    let (frac, exp) = frexp(x);
    // Make sure exact powers of two give an exact answer.
    if frac == 0.5 {
        return (exp - 1) as f64;
    }
    log(frac) * LOG2_E + exp as f64
}

/// Go `math.Log10`.
pub fn log10(x: f64) -> f64 {
    log(x) * LOG10_E
}

/// Go `math.Exp2` (pure Go on amd64).
pub fn exp2(x: f64) -> f64 {
    const LN2HI: f64 = 6.93147180369123816490e-01;
    const LN2LO: f64 = 1.90821492927058770002e-10;
    const OVERFLOW: f64 = 1.0239999999999999e+03;
    const UNDERFLOW: f64 = -1.0740e+03;
    if x.is_nan() {
        return x;
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }
    if x < UNDERFLOW {
        return 0.0;
    }
    let k: i64 = if x > 0.0 {
        (x + 0.5) as i64
    } else if x < 0.0 {
        (x - 0.5) as i64
    } else {
        0
    };
    let t = x - k as f64;
    let hi = t * LN2HI;
    let lo = -t * LN2LO;
    expmulti(hi, lo, k)
}

/// Go `expmulti`.
fn expmulti(hi: f64, lo: f64, k: i64) -> f64 {
    const P1: f64 = 1.66666666666666657415e-01;
    const P2: f64 = -2.77777777770155933842e-03;
    const P3: f64 = 6.61375632143793436117e-05;
    const P4: f64 = -1.65339022054652515390e-06;
    const P5: f64 = 4.13813679705723846039e-08;
    let r = hi - lo;
    let t = r * r;
    let c = r - t * (P1 + t * (P2 + t * (P3 + t * (P4 + t * P5))));
    let y = 1.0 - ((lo - (r * c) / (2.0 - c)) - hi);
    ldexp(y, k)
}

/// Go `math.Log1p`.
pub fn log1p(x: f64) -> f64 {
    const SQRT2M1: f64 = 4.142135623730950488017e-01;
    const SQRT2HALFM1: f64 = -2.928932188134524755992e-01;
    const SMALL: f64 = 1.0 / (1u64 << 29) as f64;
    const TINY: f64 = 1.0 / (1u64 << 54) as f64;
    const TWO53: f64 = (1u64 << 53) as f64;
    const LN2HI: f64 = 6.93147180369123816490e-01;
    const LN2LO: f64 = 1.90821492927058770002e-10;
    const LP1: f64 = 6.666666666666735130e-01;
    const LP2: f64 = 3.999999999940941908e-01;
    const LP3: f64 = 2.857142874366239149e-01;
    const LP4: f64 = 2.222219843214978396e-01;
    const LP5: f64 = 1.818357216161805012e-01;
    const LP6: f64 = 1.531383769920937332e-01;
    const LP7: f64 = 1.479819860511658591e-01;

    if x < -1.0 || x.is_nan() {
        return f64::NAN;
    }
    if x == -1.0 {
        return f64::NEG_INFINITY;
    }
    if x == f64::INFINITY {
        return f64::INFINITY;
    }
    let absx = x.abs();
    let mut f = 0.0;
    let mut iu: u64 = 0;
    let mut k: i64 = 1;
    if absx < SQRT2M1 {
        if absx < SMALL {
            if absx < TINY {
                return x;
            }
            return x - x * x * 0.5;
        }
        if x > SQRT2HALFM1 {
            k = 0;
            f = x;
            iu = 1;
        }
    }
    let mut c = 0.0;
    if k != 0 {
        let mut u;
        if absx < TWO53 {
            u = 1.0 + x;
            iu = u.to_bits();
            k = (iu >> 52) as i64 - 1023;
            if k > 0 {
                c = 1.0 - (u - x);
            } else {
                c = x - (u - 1.0);
            }
            c /= u;
        } else {
            u = x;
            iu = u.to_bits();
            k = (iu >> 52) as i64 - 1023;
            c = 0.0;
        }
        iu &= 0x000fffffffffffff;
        if iu < 0x0006a09e667f3bcd {
            u = f64::from_bits(iu | 0x3ff0000000000000);
        } else {
            k += 1;
            u = f64::from_bits(iu | 0x3fe0000000000000);
            iu = (0x0010000000000000 - iu) >> 2;
        }
        f = u - 1.0;
    }
    let hfsq = 0.5 * f * f;
    let kf = k as f64;
    if iu == 0 {
        // |f| < 2**-20
        if f == 0.0 {
            if k == 0 {
                return 0.0;
            }
            c += kf * LN2LO;
            return kf * LN2HI + c;
        }
        let r = hfsq * (1.0 - 0.66666666666666666 * f);
        if k == 0 {
            return f - r;
        }
        return kf * LN2HI - ((r - (kf * LN2LO + c)) - f);
    }
    let s = f / (2.0 + f);
    let z = s * s;
    let r = z * (LP1 + z * (LP2 + z * (LP3 + z * (LP4 + z * (LP5 + z * (LP6 + z * LP7))))));
    if k == 0 {
        return f - (hfsq - s * (hfsq + r));
    }
    kf * LN2HI - ((hfsq - (s * (hfsq + r) + (kf * LN2LO + c))) - f)
}

const SIN: [f64; 6] = [
    1.58962301576546568060e-10,
    -2.50507477628578072866e-8,
    2.75573136213857245213e-6,
    -1.98412698295895385996e-4,
    8.33333333332211858878e-3,
    -1.66666666666666307295e-1,
];
const COS: [f64; 6] = [
    -1.13585365213876817300e-11,
    2.08757008419747316778e-9,
    -2.75573141792967388112e-7,
    2.48015872888517045348e-5,
    -1.38888888888730564116e-3,
    4.16666666666665929218e-2,
];
const PI4A: f64 = 7.85398125648498535156e-1;
const PI4B: f64 = 3.77489470793079817668e-8;
const PI4C: f64 = 2.69515142907905952645e-15;
const REDUCE_THRESHOLD: f64 = (1u64 << 29) as f64;
const FOUR_OVER_PI: f64 = 4.0 * std::f64::consts::FRAC_1_PI;

/// The octant and the reduced argument (shared by sin, cos and tan).
#[inline]
fn reduce(x: f64) -> (u64, f64) {
    if x >= REDUCE_THRESHOLD {
        return trig_reduce(x);
    }
    let mut j = (x * FOUR_OVER_PI) as u64;
    let mut y = j as f64;
    if j & 1 == 1 {
        j += 1;
        y += 1.0;
    }
    (j, ((x - y * PI4A) - y * PI4B) - y * PI4C)
}

#[inline]
fn sin_poly(z: f64, zz: f64) -> f64 {
    z + z * zz * (((((SIN[0] * zz + SIN[1]) * zz + SIN[2]) * zz + SIN[3]) * zz + SIN[4]) * zz + SIN[5])
}

#[inline]
fn cos_poly(zz: f64) -> f64 {
    1.0 - 0.5 * zz + zz * zz * (((((COS[0] * zz + COS[1]) * zz + COS[2]) * zz + COS[3]) * zz + COS[4]) * zz + COS[5])
}

/// Go `math.Cos`.
pub fn cos(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
    }
    let mut sign = false;
    let x = x.abs();
    let (mut j, z) = reduce(x);
    j &= 7;
    if j > 3 {
        j -= 4;
        sign = !sign;
    }
    if j > 1 {
        sign = !sign;
    }
    let zz = z * z;
    let y = if j == 1 || j == 2 { sin_poly(z, zz) } else { cos_poly(zz) };
    if sign { -y } else { y }
}

/// Go `math.Sin`.
pub fn sin(x: f64) -> f64 {
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return f64::NAN;
    }
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    let (mut j, z) = reduce(x);
    j &= 7;
    if j > 3 {
        sign = !sign;
        j -= 4;
    }
    let zz = z * z;
    let y = if j == 1 || j == 2 { cos_poly(zz) } else { sin_poly(z, zz) };
    if sign { -y } else { y }
}

/// Go `math.Tan`.
pub fn tan(x: f64) -> f64 {
    const TAN_P: [f64; 3] = [-1.30936939181383777646e4, 1.15351664838587416140e6, -1.79565251976484877988e7];
    const TAN_Q: [f64; 5] = [
        1.00000000000000000000e0,
        1.36812963470692954678e4,
        -1.32089234440210967447e6,
        2.50083801823357915839e7,
        -5.38695755929454629881e7,
    ];
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return f64::NAN;
    }
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    // Go does not reduce the octant modulo 8 here (only bit 1 is used).
    let (j, z) = reduce(x);
    let zz = z * z;
    let mut y = if zz > 1e-14 {
        z + z * (zz * ((TAN_P[0] * zz + TAN_P[1]) * zz + TAN_P[2])
            / ((((zz + TAN_Q[1]) * zz + TAN_Q[2]) * zz + TAN_Q[3]) * zz + TAN_Q[4]))
    } else {
        z
    };
    if j & 2 == 2 {
        y = -1.0 / y;
    }
    if sign { -y } else { y }
}

const M_PI4: [u64; 20] = [
    0x0000000000000001,
    0x45f306dc9c882a53,
    0xf84eafa3ea69bb81,
    0xb6c52b3278872083,
    0xfca2c757bd778ac3,
    0x6e48dc74849ba5c0,
    0x0c925dd413a32439,
    0xfc3bd63962534e7d,
    0xd1046bea5d768909,
    0xd338e04d68befc82,
    0x7323ac7306a673e9,
    0x3908bf177bf25076,
    0x3ff12fffbc0b301f,
    0xde5e2316b414da3e,
    0xda6cfd9e4f96136e,
    0x9e8c7ecd3cbfd45a,
    0xea4f758fd7cbe2f6,
    0x7a0e73ef14a525d4,
    0xd7f6bf623f1aba10,
    0xac06608df8f6d757,
];

#[inline]
fn shl(x: u64, n: u32) -> u64 {
    x.checked_shl(n).unwrap_or(0)
}

#[inline]
fn shr(x: u64, n: u32) -> u64 {
    x.checked_shr(n).unwrap_or(0)
}

/// Go `trigReduce`: Payne-Hanek reduction for large arguments.
fn trig_reduce(x: f64) -> (u64, f64) {
    if x < FRAC_PI_4 {
        return (0, x);
    }
    let mut ix = x.to_bits();
    let exp = ((ix >> SHIFT) & MASK) as i64 - BIAS - SHIFT as i64;
    ix &= !(MASK << SHIFT);
    ix |= 1 << SHIFT;
    let digit = ((exp + 61) as u64 / 64) as usize;
    let bitshift = ((exp + 61) as u64 % 64) as u32;
    let z0 = shl(M_PI4[digit], bitshift) | shr(M_PI4[digit + 1], 64 - bitshift);
    let z1 = shl(M_PI4[digit + 1], bitshift) | shr(M_PI4[digit + 2], 64 - bitshift);
    let z2 = shl(M_PI4[digit + 2], bitshift) | shr(M_PI4[digit + 3], 64 - bitshift);
    let z2hi = ((z2 as u128 * ix as u128) >> 64) as u64;
    let p1 = z1 as u128 * ix as u128;
    let (z1hi, z1lo) = ((p1 >> 64) as u64, p1 as u64);
    let z0lo = z0.wrapping_mul(ix);
    let (lo, c) = z1lo.overflowing_add(z2hi);
    let hi = z0lo.wrapping_add(z1hi).wrapping_add(c as u64);
    let mut j = hi >> 61;
    let mut hi = hi << 3 | lo >> 61;
    let lz = hi.leading_zeros();
    let e = (BIAS as u64).wrapping_sub(lz as u64 + 1);
    hi = shl(hi, lz + 1) | shr(lo, 64 - (lz + 1));
    hi >>= 64 - SHIFT;
    hi |= e << SHIFT;
    let mut z = f64::from_bits(hi);
    if j & 1 == 1 {
        j += 1;
        j &= 7;
        z -= 1.0;
    }
    (j, z * FRAC_PI_4)
}

/// Go `xatan`.
fn xatan(x: f64) -> f64 {
    const P0: f64 = -8.750608600031904122785e-01;
    const P1: f64 = -1.615753718733365076637e+01;
    const P2: f64 = -7.500855792314704667340e+01;
    const P3: f64 = -1.228866684490136173410e+02;
    const P4: f64 = -6.485021904942025371773e+01;
    const Q0: f64 = 2.485846490142306297962e+01;
    const Q1: f64 = 1.650270098316988542046e+02;
    const Q2: f64 = 4.328810604912902668951e+02;
    const Q3: f64 = 4.853903996359136964868e+02;
    const Q4: f64 = 1.945506571482613964425e+02;
    let mut z = x * x;
    z = z * ((((P0 * z + P1) * z + P2) * z + P3) * z + P4) / (((((z + Q0) * z + Q1) * z + Q2) * z + Q3) * z + Q4);
    x * z + x
}

/// Go `satan`.
fn satan(x: f64) -> f64 {
    const MOREBITS: f64 = 6.123233995736765886130e-17;
    const TAN3PIO8: f64 = 2.41421356237309504880;
    if x <= 0.66 {
        return xatan(x);
    }
    if x > TAN3PIO8 {
        return FRAC_PI_2 - xatan(1.0 / x) + MOREBITS;
    }
    FRAC_PI_4 + xatan((x - 1.0) / (x + 1.0)) + 0.5 * MOREBITS
}

/// Go `math.Atan`.
pub fn atan(x: f64) -> f64 {
    if x == 0.0 {
        return x;
    }
    if x > 0.0 {
        return satan(x);
    }
    -satan(-x)
}

/// Go `math.Asin`.
pub fn asin(x: f64) -> f64 {
    if x == 0.0 {
        return x;
    }
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    if x > 1.0 {
        return f64::NAN;
    }
    let mut temp = (1.0 - x * x).sqrt();
    if x > 0.7 {
        temp = FRAC_PI_2 - satan(temp / x);
    } else {
        temp = satan(x / temp);
    }
    if sign { -temp } else { temp }
}

/// Go `math.Acos`.
pub fn acos(x: f64) -> f64 {
    FRAC_PI_2 - asin(x)
}

/// Go `math.Sinh`.
pub fn sinh(x: f64) -> f64 {
    const P0: f64 = -0.6307673640497716991184787251e+6;
    const P1: f64 = -0.8991272022039509355398013511e+5;
    const P2: f64 = -0.2894211355989563807284660366e+4;
    const P3: f64 = -0.2630563213397497062819489e+2;
    const Q0: f64 = -0.6307673640497716991212077277e+6;
    const Q1: f64 = 0.1521517378790019070696485176e+5;
    const Q2: f64 = -0.173678953558233699533450911e+3;
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    let temp = if x > 21.0 {
        exp(x) * 0.5
    } else if x > 0.5 {
        let ex = exp(x);
        (ex - 1.0 / ex) * 0.5
    } else {
        let sq = x * x;
        let t = (((P3 * sq + P2) * sq + P1) * sq + P0) * x;
        t / (((sq + Q2) * sq + Q1) * sq + Q0)
    };
    if sign { -temp } else { temp }
}

/// Go `math.Cosh`.
pub fn cosh(x: f64) -> f64 {
    let x = x.abs();
    if x > 21.0 {
        return exp(x) * 0.5;
    }
    let ex = exp(x);
    (ex + 1.0 / ex) * 0.5
}

/// Go `math.Tanh`.
pub fn tanh(x: f64) -> f64 {
    const P: [f64; 3] = [-9.64399179425052238628e-1, -9.92877231001918586564e1, -1.61468768441708447952e3];
    const Q: [f64; 3] = [1.12811678491632931402e2, 2.23548839060100448583e3, 4.84406305325125486048e3];
    const MAXLOG: f64 = 8.8029691931113054295988e+01;
    let mut z = x.abs();
    if z > 0.5 * MAXLOG {
        if x < 0.0 {
            return -1.0;
        }
        return 1.0;
    } else if z >= 0.625 {
        let s = exp(2.0 * z);
        z = 1.0 - 2.0 / (s + 1.0);
        if x < 0.0 {
            z = -z;
        }
    } else {
        if x == 0.0 {
            return x;
        }
        let s = x * x;
        z = x + x * s * ((P[0] * s + P[1]) * s + P[2]) / (((s + Q[0]) * s + Q[1]) * s + Q[2]);
    }
    z
}

/// Go `math.Asinh`.
pub fn asinh(x: f64) -> f64 {
    const NEAR_ZERO: f64 = 1.0 / (1u64 << 28) as f64;
    const LARGE: f64 = (1u64 << 28) as f64;
    if x.is_nan() || x.is_infinite() {
        return x;
    }
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    let temp = if x > LARGE {
        log(x) + LN_2
    } else if x > 2.0 {
        log(2.0 * x + 1.0 / ((x * x + 1.0).sqrt() + x))
    } else if x < NEAR_ZERO {
        x
    } else {
        log1p(x + x * x / (1.0 + (1.0 + x * x).sqrt()))
    };
    if sign { -temp } else { temp }
}

/// Go `math.Acosh`.
pub fn acosh(x: f64) -> f64 {
    const LARGE: f64 = (1u64 << 28) as f64;
    if x < 1.0 || x.is_nan() {
        return f64::NAN;
    }
    if x == 1.0 {
        return 0.0;
    }
    if x >= LARGE {
        return log(x) + LN_2;
    }
    if x > 2.0 {
        return log(2.0 * x - 1.0 / (x + (x * x - 1.0).sqrt()));
    }
    let t = x - 1.0;
    log1p(t + (2.0 * t + t * t).sqrt())
}

/// Go `math.Atanh`.
pub fn atanh(x: f64) -> f64 {
    const NEAR_ZERO: f64 = 1.0 / (1u64 << 28) as f64;
    if x < -1.0 || x > 1.0 || x.is_nan() {
        return f64::NAN;
    }
    if x == 1.0 {
        return f64::INFINITY;
    }
    if x == -1.0 {
        return f64::NEG_INFINITY;
    }
    let mut sign = false;
    let mut x = x;
    if x < 0.0 {
        x = -x;
        sign = true;
    }
    let temp = if x < NEAR_ZERO {
        x
    } else if x < 0.5 {
        let t = x + x;
        0.5 * log1p(t + t * x / (1.0 - x))
    } else {
        0.5 * log1p((x + x) / (1.0 - x))
    };
    if sign { -temp } else { temp }
}
