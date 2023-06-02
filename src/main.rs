use async_std::net::TcpStream;
use rust_xlsxwriter::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tiberius::{numeric::Numeric, time, AuthMethod, Client, ColumnType, Config, Query};

#[derive(Serialize, Deserialize, Debug)]
struct Settings {
    database: String,
    host: String,
    port: u16,
    username: String,
    password: String,
    output: String,
}

#[async_std::main]
async fn main() -> Result<(), anyhow::Error> {
    // Using the builder method to construct the options.
    let mut config = Config::new();

    let file_contents =
        std::fs::read_to_string("settings.json").expect("Should have been able to read the file");
    let settings: Settings =
        serde_json::from_str(&file_contents).expect("Should have been able to parse the file");

    config.database(settings.database);
    config.host(settings.host);
    config.port(settings.port);

    // Using SQL Server authentication.
    config.authentication(AuthMethod::sql_server(settings.username, settings.password));

    // on production, it is not a good idea to do this
    config.trust_cert();

    // Taking the address from the configuration, using async-std's
    // TcpStream to connect to the server.
    let tcp = TcpStream::connect(config.get_addr()).await?;

    // We'll disable the Nagle algorithm. Buffering is handled
    // internally with a `Sink`.
    tcp.set_nodelay(true)?;

    // Handling TLS, login and other details related to the SQL Server.
    let mut client = Client::connect(config, tcp).await?;

    let sql_file_path = Path::new("query.sql");
    let mut sql_file = File::open(sql_file_path).unwrap();
    let mut sql_query = String::new();
    sql_file.read_to_string(&mut sql_query).unwrap();

    // Constructing a query object with one parameter annotated with `@P1`.
    // This requires us to bind a parameter that will then be used in
    // the statement.
    let select = Query::new(sql_query);

    // A response to a query is a stream of data, that must be
    // polled to the end before querying again. Using streams allows
    // fetching data in an asynchronous manner, if needed.
    let stream = select.query(&mut client).await?;

    // In this case, we know we have only one query, returning one row
    // and one column, so calling `into_row` will consume the stream
    // and return us the first row of the first result.
    // let row = stream.into_row().await?;

    // print!("{:#?}",row);

    let results = stream.into_results().await?;

    // results.into_iter().for_each( | row | {
    // results.first( | result | {
    //     result.into_iter().for_each( | row | {
    //         println!("This is a row {:#?}",row);
    //     });
    // });

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    results
        .first()
        .unwrap()
        .into_iter()
        .enumerate()
        .for_each(|(index_row, row)| {
            // println!("This is a row!: {:#?}", index_row);
            if index_row == 0 {
                for (index_column, column) in row.columns().iter().enumerate() {
                    let _ = worksheet.write(index_row as u32, index_column as u16, column.name());
                }
            }
            for (index_column, column) in row.columns().iter().enumerate() {
                // let value = row.try_get(index_column);
                // println!("{:#?}",column);
                // worksheet.write(index_column as u32 , index_row as u16 , value );
                match column.column_type() {
                    ColumnType::Numericn => {
                        // println!("this is a numericcc!!");
                        let value = row.try_get::<Numeric, usize>(index_column);
                        if let Some(real_value) = value.unwrap() {
                            // println!("real value: {:#?}", real_value.value());
                            let _ = worksheet.write(
                                index_row as u32 + 1,
                                index_column as u16,
                                real_value.value() as i32,
                            );
                        }
                        // println!("val:{:#?}",value.unwrap());
                    }
                    ColumnType::Bitn => {
                        // println!("this is a bitn!!");
                        let value = row.try_get::<bool, usize>(index_column);
                        if let Some(real_value) = value.unwrap() {
                            // println!("real value: {:#?}", real_value);
                            let _ = worksheet.write(index_row as u32 + 1, index_column as u16, real_value);
                        }
                        // println!("val:{:#?}",value.unwrap());
                    }
                    ColumnType::Intn => {
                        // println!("this is a int!!");
                        let got_error = false;
                        let value = row.try_get::<u8, usize>(index_column);
                        // println!("after u8: {:#?}",value);
                        let real_value = match value {
                            Ok(num) => {
                                if let Some(real_value) = value.unwrap() {
                                    // println!("real value u8: {:#?}", real_value);
                                    worksheet.write(
                                        index_row as u32 + 1,
                                        index_column as u16,
                                        real_value,
                                    );
                                }
                            }
                            Err(_) => {
                                let value = row.try_get::<i64, usize>(index_column);
                                // println!("after i64: {:#?}",value);
                                let real_value = match value {
                                    Ok(num) => {
                                        if let Some(real_value) = value.unwrap() {
                                            // println!("real value i64: {:#?}", real_value);
                                            worksheet.write(
                                                index_row as u32 + 1,
                                                index_column as u16,
                                                real_value as i32,
                                            );
                                        }
                                    }
                                    Err(_) => continue,
                                };
                            }
                        };
                        // println!("after match: {:#?}",real_value);
                        // if let Some(real_value) = value.unwrap() {
                        //     println!("real value: {:#?}", real_value);
                        //     // worksheet.write( index_row as u32, index_column as u16 , real_value.value() as i32 );
                        // }
                        // println!("val:{:#?}",value.unwrap());
                    }
                    ColumnType::BigVarChar | ColumnType::NVarchar => {
                        // println!("this is a string!!");
                        let value = row.try_get::<&str, usize>(index_column);
                        if let Some(real_value) = value.unwrap() {
                            // println!("real value: {:#?}", real_value);
                            worksheet.write(index_row as u32 + 1, index_column as u16, real_value);
                        }
                        // println!("val:{:#?}",value.unwrap());
                    }
                    ColumnType::Datetime2 | ColumnType::Datetimen => {
                        // println!("this is a datetime!!");
                        let value = row.try_get::<time::chrono::NaiveDateTime, usize>(index_column);
                        if let Some(real_value) = value.unwrap() {
                            // println!("real value: {:#?}", real_value.to_string());
                            worksheet.write(
                                index_row as u32 + 1,
                                index_column as u16,
                                real_value.to_string(),
                            );
                        }
                        // println!("val:{:#?}",value.unwrap());
                    }
                    _ => {
                        println!("i dont know what this is");
                        println!("{:#?}", column);
                    }
                }
            }
        });

    workbook.save(settings.output);

    Ok(())
}
