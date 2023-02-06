use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{api, define_scalar_function, Error, Result};

#[sqlite_entrypoint]
fn sqlite3_deflate_init(db: *mut sqlite3) -> Result<()> {
    let flags = FunctionFlags::UTF8 | FunctionFlags::DETERMINISTIC;
    define_scalar_function(db, "deflate", 1, deflate, flags)?;
    define_scalar_function(db, "inflate", 1, inflate, flags)?;
    Ok(())
}

pub fn deflate(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let contents = api::value_blob(
        values
            .get(0)
            .ok_or_else(|| Error::new_message("expected 1st argument as contents"))?,
    );
    let res = compress_to_vec(contents, 6);
    api::result_blob(context, &res);
    Ok(())
}

pub fn inflate(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let contents = api::value_blob(
        values
            .get(0)
            .ok_or_else(|| Error::new_message("expected 1st argument as contents"))?,
    );
    let res = decompress_to_vec(contents).map_err(|err| Error::from(err.to_string()))?;
    api::result_blob(context, &res);
    Ok(())
}
