use std::io::{self, Cursor};

use brotli::enc::BrotliEncoderParams;
use rusqlite::{Connection, functions::FunctionFlags};

pub fn add_brotli_functions(db: &Connection) -> rusqlite::Result<()> {
    db.create_scalar_function(
        "brotli_compress",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: &[u8] = ctx.get_raw(0).as_bytes()?;
            Ok(brotli_compress(value)?)
        }
    )?;

    db.create_scalar_function(
        "brotli_decompress",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: &[u8] = ctx.get_raw(0).as_blob()?;
            Ok(brotli_decompress(value)?)
        }
    )?;

    Ok(())
}

fn brotli_compress(input: &[u8]) -> rusqlite::Result<Vec<u8>> {
    let mut input = Cursor::new(input);
    let mut output = Vec::new();
    let params = BrotliEncoderParams::default();
    brotli::BrotliCompress(&mut input, &mut output, &params)
        .map_err(io_error_to_rusqlite)?;
    Ok(output)
}

fn brotli_decompress(input: &[u8]) -> rusqlite::Result<Vec<u8>> {
    let mut input = Cursor::new(input);
    let mut output = Vec::new();
    brotli::BrotliDecompress(&mut input, &mut output)
        .map_err(io_error_to_rusqlite)?;
    Ok(output)
}

fn io_error_to_rusqlite(err: io::Error) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(err))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::add_brotli_functions;

    #[test]
    fn test_brotli_compress() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        add_brotli_functions(&db)?;

        let _b = db.query_row(r##"SELECT brotli_compress("aaaaaaaaa")"##, [], |row| {
            let b: Vec<u8> = row.get(0)?;
            Ok(b)
        })?;

        Ok(())
    }

    #[test]
    fn test_brotli_decompress() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        add_brotli_functions(&db)?;

        db.execute("CREATE TABLE brotli (bloerp);", [])?;
        db.execute("INSERT INTO brotli VALUES(brotli_compress('aaaaaaaaa'));", [])?;
        let bytes = db.query_row(r"SELECT brotli_decompress(bloerp) FROM brotli", [], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(bytes)
        })?;

        let s = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(s, "aaaaaaaaa");

        Ok(())
    }

    quickcheck::quickcheck! {
        #[test]
        fn prop_brotli_compress_decompress(xs: String) -> bool {
            let db = Connection::open_in_memory().unwrap();
            add_brotli_functions(&db).unwrap();

            db.execute("CREATE TABLE brotli (bloerp);", []).unwrap();
            db.execute("INSERT INTO brotli VALUES(brotli_compress(?));", [&xs]).unwrap();

            let res: String = db.query_row(r"SELECT brotli_decompress(bloerp) FROM brotli", [], |row| {
                let bytes = row.get(0)?;
                Ok(String::from_utf8(bytes).unwrap())
            }).unwrap();

            xs == res
        }
    }
}
