//! RSA private keys for the OCI request signer: the port of oci-go-sdk `common.PrivateKeyFromBytesWithPassword`
//! (PEM, PKCS#1, PKCS#8, encrypted PKCS#8 from github.com/youmark/pkcs8, legacy encrypted PEM from
//! `x509.DecryptPEMBlock`). The key is handed to ring for signing.
//!
//! Supported encryptions: PKCS#8 PBES2 with PBKDF2 (HMAC-SHA1 or HMAC-SHA256) and AES-128/192/256-CBC, and
//! legacy PEM (`Proc-Type: 4,ENCRYPTED`) with AES-128/192/256-CBC. Not supported (a clear error): DES and
//! 3DES ciphers, AES-GCM in PKCS#8, the scrypt KDF.
use ring::signature::RsaKeyPair;

/// A PEM block (Go `pem.Block`).
pub struct PemBlock {
    pub type_: String,
    pub headers: Vec<(String, String)>,
    pub bytes: Vec<u8>,
}

/// Go `pem.Decode` for the first block in `data`.
pub fn pem_decode(data: &[u8]) -> Option<PemBlock> {
    let text = String::from_utf8_lossy(data);
    let mut rest: &str = &text;
    while let Some(i) = rest.find("-----BEGIN ") {
        let after = &rest[i + 11..];
        let j = after.find("-----")?;
        let type_ = after[..j].to_string();
        let body = &after[j + 5..];
        let end_marker = format!("-----END {type_}-----");
        let Some(k) = body.find(&end_marker) else {
            rest = body;
            continue;
        };
        let mut headers = Vec::new();
        let mut b64 = String::new();
        let mut in_headers = true;
        for line in body[..k].lines() {
            let line = line.trim_end_matches('\r');
            if in_headers {
                if let Some((hk, hv)) = line.split_once(':') {
                    headers.push((hk.trim().to_string(), hv.trim().to_string()));
                    continue;
                }
                if !headers.is_empty() && line.trim().is_empty() {
                    in_headers = false;
                    continue;
                }
                if !line.trim().is_empty() {
                    in_headers = false;
                }
            }
            b64.extend(line.chars().filter(|c| !c.is_whitespace()));
        }
        match crypto::base64_decode(&b64) {
            Some(bytes) => return Some(PemBlock { type_, headers, bytes }),
            None => {
                rest = &body[k..];
                continue;
            }
        }
    }
    None
}

/// Go `common.PrivateKeyFromBytesWithPassword`: the RSA key in `pem_data`, decrypted with `password` if set.
pub fn private_key_from_bytes(pem_data: &[u8], password: Option<&[u8]>) -> Result<RsaKeyPair, String> {
    let Some(block) = pem_decode(pem_data) else {
        return Err("PEM data was not found in buffer".to_string());
    };
    // Encrypted or plain PKCS#8 (github.com/youmark/pkcs8).
    if let Ok(pkcs1) = parse_pkcs8_rsa(&block.bytes, password.unwrap_or_default()) {
        return key_pair(&pkcs1);
    }
    let mut decrypted = block.bytes.clone();
    if block.headers.iter().any(|(k, _)| k == "DEK-Info") {
        let Some(pw) = password else {
            return Err("private key password is required for encrypted private keys".to_string());
        };
        decrypted = decrypt_pem_block(&block, pw)?;
    }
    let pkcs1 = parse_pkcs_private_key(&decrypted)?;
    key_pair(&pkcs1)
}

fn key_pair(pkcs1: &[u8]) -> Result<RsaKeyPair, String> {
    RsaKeyPair::from_der(pkcs1).map_err(|e| format!("unsupported RSA private key: {e}"))
}

// ---------------- DER ----------------

/// One DER element: tag, contents and the rest of the input.
fn der_next(d: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = d.split_first()?;
    let (&l0, mut rest) = rest.split_first()?;
    let len = if l0 < 0x80 {
        l0 as usize
    } else {
        let n = (l0 & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let mut l = 0usize;
        for &b in &rest[..n] {
            l = (l << 8) | b as usize;
        }
        rest = &rest[n..];
        l
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

/// The contents of an element with `tag` at the start of `d`, and the rest.
fn der_expect(d: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (t, c, r) = der_next(d)?;
    (t == tag).then_some((c, r))
}

const SEQ: u8 = 0x30;
const INT: u8 = 0x02;
const OCTETS: u8 = 0x04;
const OID: u8 = 0x06;

const OID_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];
const OID_PBES2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x05, 0x0d];
const OID_PBKDF2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x05, 0x0c];
const OID_HMAC_SHA1: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x02, 0x07];
const OID_HMAC_SHA256: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x02, 0x09];
const OID_AES128_CBC: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x01, 0x02];
const OID_AES192_CBC: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x01, 0x16];
const OID_AES256_CBC: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x01, 0x2a];

/// A PKCS#1 RSAPrivateKey: a sequence of a version and eight integers.
fn is_pkcs1(d: &[u8]) -> bool {
    let Some((mut c, rest)) = der_expect(d, SEQ) else { return false };
    if !rest.is_empty() {
        return false;
    }
    for _ in 0..9 {
        match der_expect(c, INT) {
            Some((_, r)) => c = r,
            None => return false,
        }
    }
    true
}

/// A PKCS#8 PrivateKeyInfo: the algorithm OID and the inner key.
fn parse_pkcs8(d: &[u8]) -> Option<(&[u8], &[u8])> {
    let (c, _) = der_expect(d, SEQ)?;
    let (_, c) = der_expect(c, INT)?;
    let (alg, c) = der_expect(c, SEQ)?;
    let (oid, _) = der_expect(alg, OID)?;
    let (key, _) = der_expect(c, OCTETS)?;
    Some((oid, key))
}

/// Go `x509.ParsePKCS8PrivateKey` restricted to RSA: the PKCS#1 key inside.
fn pkcs8_rsa(d: &[u8]) -> Result<Vec<u8>, String> {
    let Some((oid, key)) = parse_pkcs8(d) else {
        return Err("x509: failed to parse private key".to_string());
    };
    if oid != OID_RSA {
        return Err("key block is not of type RSA".to_string());
    }
    if !is_pkcs1(key) {
        return Err("x509: failed to parse RSA private key embedded in PKCS#8".to_string());
    }
    Ok(key.to_vec())
}

/// Go `pkcs8.ParsePKCS8PrivateKeyRSA(der, password)`.
fn parse_pkcs8_rsa(der: &[u8], password: &[u8]) -> Result<Vec<u8>, String> {
    if password.is_empty() {
        return pkcs8_rsa(der);
    }
    let decrypted = decrypt_pkcs8(der, password)?;
    pkcs8_rsa(&decrypted).map_err(|_| "pkcs8: incorrect password".to_string())
}

/// Go `parsePKCSPrivateKey`: PKCS#1, then PKCS#8.
fn parse_pkcs_private_key(d: &[u8]) -> Result<Vec<u8>, String> {
    if is_pkcs1(d) {
        return Ok(d.to_vec());
    }
    if let Some((oid, key)) = parse_pkcs8(d) {
        if oid != OID_RSA {
            return Err("unsupportesd private key type in PKCS8 wrapping".to_string());
        }
        if is_pkcs1(key) {
            return Ok(key.to_vec());
        }
    }
    Err("failed to parse private key".to_string())
}

/// PBES2 decryption of an EncryptedPrivateKeyInfo (github.com/youmark/pkcs8 `ParsePrivateKey`).
fn decrypt_pkcs8(der: &[u8], password: &[u8]) -> Result<Vec<u8>, String> {
    let parsed = (|| {
        let (c, _) = der_expect(der, SEQ)?;
        let (alg, c) = der_expect(c, SEQ)?;
        let (data, _) = der_expect(c, OCTETS)?;
        let (oid, params) = der_expect(alg, OID)?;
        Some((oid, params, data))
    })();
    let Some((oid, params, data)) = parsed else {
        return Err("pkcs8: only PKCS #5 v2.0 supported".to_string());
    };
    if oid != OID_PBES2 {
        return Err("pkcs8: only PBES2 supported".to_string());
    }
    let scheme = (|| {
        let (p, _) = der_expect(params, SEQ)?;
        let (kdf, p) = der_expect(p, SEQ)?;
        let (enc, _) = der_expect(p, SEQ)?;
        Some((kdf, enc))
    })();
    let Some((kdf, enc)) = scheme else {
        return Err("pkcs8: invalid PBES2 parameters".to_string());
    };
    // Encryption scheme: AES-CBC with a 16-byte IV.
    let (enc_oid, enc_rest) = der_expect(enc, OID).ok_or("pkcs8: invalid PBES2 parameters")?;
    let key_size = match enc_oid {
        OID_AES128_CBC => 16,
        OID_AES192_CBC => 24,
        OID_AES256_CBC => 32,
        _ => return Err("pkcs8: unsupported encryption scheme (prometheus-rs decrypts only AES-128/192/256-CBC)".to_string()),
    };
    let (iv, _) = der_expect(enc_rest, OCTETS).ok_or("pkcs8: invalid cipher parameters")?;
    // Key derivation: PBKDF2 with HMAC-SHA1 (default) or HMAC-SHA256.
    let (kdf_oid, kdf_rest) = der_expect(kdf, OID).ok_or("pkcs8: invalid PBES2 parameters")?;
    if kdf_oid != OID_PBKDF2 {
        return Err("pkcs8: unsupported key derivation function (prometheus-rs supports only PBKDF2)".to_string());
    }
    let (kp, _) = der_expect(kdf_rest, SEQ).ok_or("pkcs8: invalid PBKDF2 parameters")?;
    let (salt, kp) = der_expect(kp, OCTETS).ok_or("pkcs8: invalid PBKDF2 parameters")?;
    let (iter, mut kp) = der_expect(kp, INT).ok_or("pkcs8: invalid PBKDF2 parameters")?;
    let iterations = iter.iter().fold(0u64, |a, &b| (a << 8) | b as u64);
    if let Some((_, r)) = der_expect(kp, INT) {
        kp = r; // optional key length
    }
    let prf = match der_expect(kp, SEQ) {
        Some((p, _)) => der_expect(p, OID).map(|(o, _)| o).ok_or("pkcs8: invalid PBKDF2 parameters")?,
        None => OID_HMAC_SHA1,
    };
    let alg = match prf {
        OID_HMAC_SHA1 => ring::pbkdf2::PBKDF2_HMAC_SHA1,
        OID_HMAC_SHA256 => ring::pbkdf2::PBKDF2_HMAC_SHA256,
        _ => return Err("pkcs8: unsupported hash function".to_string()),
    };
    let iterations = std::num::NonZeroU32::new(iterations.min(u32::MAX as u64) as u32).ok_or("pkcs8: invalid PBKDF2 parameters")?;
    let mut key = vec![0u8; key_size];
    ring::pbkdf2::derive(alg, iterations, salt, password, &mut key);
    let plain = aes_cbc_decrypt(&key, iv, data).ok_or("pkcs8: invalid cipher parameters")?;
    unpad(plain).ok_or_else(|| "pkcs8: incorrect password".to_string())
}

/// Go `x509.DecryptPEMBlock` for the AES-CBC modes.
fn decrypt_pem_block(b: &PemBlock, password: &[u8]) -> Result<Vec<u8>, String> {
    let dek = b.headers.iter().find(|(k, _)| k == "DEK-Info").map(|(_, v)| v.as_str()).unwrap_or("");
    let Some((mode, hex_iv)) = dek.split_once(',') else {
        return Err("x509: malformed DEK-Info header".to_string());
    };
    let key_size = match mode.trim() {
        "AES-128-CBC" => 16,
        "AES-192-CBC" => 24,
        "AES-256-CBC" => 32,
        "DES-CBC" | "DES-EDE3-CBC" => return Err(format!("x509: {} encrypted PEM keys are not supported by prometheus-rs (only AES-128/192/256-CBC)", mode.trim())),
        _ => return Err("x509: unknown encryption mode".to_string()),
    };
    let Some(iv) = crypto::hex_decode(hex_iv.trim()) else {
        return Err("x509: malformed DEK-Info header".to_string());
    };
    if iv.len() != 16 {
        return Err("x509: incorrect IV size".to_string());
    }
    // OpenSSL EVP_BytesToKey with MD5, one iteration, the first 8 IV bytes as salt.
    let mut key = Vec::with_capacity(key_size + 16);
    let mut prev: Vec<u8> = Vec::new();
    while key.len() < key_size {
        let mut d = prev.clone();
        d.extend_from_slice(password);
        d.extend_from_slice(&iv[..8]);
        prev = crypto::md5(&d).to_vec();
        key.extend_from_slice(&prev);
    }
    key.truncate(key_size);
    if !b.bytes.len().is_multiple_of(16) {
        return Err("x509: encrypted PEM data is not a multiple of the block size".to_string());
    }
    let plain = aes_cbc_decrypt(&key, &iv, &b.bytes).ok_or("x509: invalid padding")?;
    let Some(&last) = plain.last() else {
        return Err("x509: invalid padding".to_string());
    };
    let n = last as usize;
    if n == 0 || n > 16 || n > plain.len() {
        return Err("x509: decryption password incorrect".to_string());
    }
    if plain[plain.len() - n..].iter().any(|&x| x as usize != n) {
        return Err("x509: decryption password incorrect".to_string());
    }
    Ok(plain[..plain.len() - n].to_vec())
}

/// Strips PKCS#7 padding.
fn unpad(mut p: Vec<u8>) -> Option<Vec<u8>> {
    let n = *p.last()? as usize;
    if n == 0 || n > 16 || n > p.len() || p[p.len() - n..].iter().any(|&x| x as usize != n) {
        return None;
    }
    p.truncate(p.len() - n);
    Some(p)
}

// ---------------- AES decryption (FIPS 197), CBC mode ----------------

fn xtime(a: u8) -> u8 {
    (a << 1) ^ if a & 0x80 != 0 { 0x1b } else { 0 }
}

fn gmul(mut a: u8, mut b: u8) -> u8 {
    let mut p = 0u8;
    while b != 0 {
        if b & 1 != 0 {
            p ^= a;
        }
        a = xtime(a);
        b >>= 1;
    }
    p
}

fn sboxes() -> ([u8; 256], [u8; 256]) {
    let mut s = [0u8; 256];
    let mut inv = [0u8; 256];
    #[allow(clippy::needless_range_loop)]
    for x in 0..256usize {
        // multiplicative inverse in GF(2^8), then the affine map
        let mut y = 0u8;
        if x != 0 {
            for c in 1..=255u8 {
                if gmul(x as u8, c) == 1 {
                    y = c;
                    break;
                }
            }
        }
        let v = y ^ y.rotate_left(1) ^ y.rotate_left(2) ^ y.rotate_left(3) ^ y.rotate_left(4) ^ 0x63;
        s[x] = v;
        inv[v as usize] = x as u8;
    }
    (s, inv)
}

/// AES-CBC decryption without unpadding; `None` for a bad key, IV or length.
fn aes_cbc_decrypt(key: &[u8], iv: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    let nk = key.len() / 4;
    if !matches!(key.len(), 16 | 24 | 32) || iv.len() != 16 || data.is_empty() || !data.len().is_multiple_of(16) {
        return None;
    }
    let (s, inv) = sboxes();
    let nr = nk + 6;
    let mut w: Vec<[u8; 4]> = key.chunks(4).map(|c| [c[0], c[1], c[2], c[3]]).collect();
    let mut rcon = 1u8;
    for i in nk..4 * (nr + 1) {
        let mut t = w[i - 1];
        if i % nk == 0 {
            t = [s[t[1] as usize] ^ rcon, s[t[2] as usize], s[t[3] as usize], s[t[0] as usize]];
            rcon = xtime(rcon);
        } else if nk > 6 && i % nk == 4 {
            t = [s[t[0] as usize], s[t[1] as usize], s[t[2] as usize], s[t[3] as usize]];
        }
        let p = w[i - nk];
        w.push([p[0] ^ t[0], p[1] ^ t[1], p[2] ^ t[2], p[3] ^ t[3]]);
    }
    let add_round_key = |st: &mut [u8; 16], r: usize| {
        for c in 0..4 {
            for j in 0..4 {
                st[4 * c + j] ^= w[4 * r + c][j];
            }
        }
    };
    let mut out = Vec::with_capacity(data.len());
    let mut prev = [0u8; 16];
    prev.copy_from_slice(iv);
    for block in data.chunks(16) {
        let mut st = [0u8; 16];
        st.copy_from_slice(block);
        add_round_key(&mut st, nr);
        for round in (0..nr).rev() {
            // InvShiftRows and InvSubBytes (state byte r + 4c is row r, column c)
            let mut t = [0u8; 16];
            for c in 0..4 {
                for r in 0..4 {
                    t[r + 4 * ((c + r) % 4)] = inv[st[r + 4 * c] as usize];
                }
            }
            st = t;
            add_round_key(&mut st, round);
            if round > 0 {
                for c in 0..4 {
                    let a = [st[4 * c], st[4 * c + 1], st[4 * c + 2], st[4 * c + 3]];
                    st[4 * c] = gmul(a[0], 14) ^ gmul(a[1], 11) ^ gmul(a[2], 13) ^ gmul(a[3], 9);
                    st[4 * c + 1] = gmul(a[0], 9) ^ gmul(a[1], 14) ^ gmul(a[2], 11) ^ gmul(a[3], 13);
                    st[4 * c + 2] = gmul(a[0], 13) ^ gmul(a[1], 9) ^ gmul(a[2], 14) ^ gmul(a[3], 11);
                    st[4 * c + 3] = gmul(a[0], 11) ^ gmul(a[1], 13) ^ gmul(a[2], 9) ^ gmul(a[3], 14);
                }
            }
        }
        for j in 0..16 {
            out.push(st[j] ^ prev[j]);
        }
        prev.copy_from_slice(block);
    }
    Some(out)
}
