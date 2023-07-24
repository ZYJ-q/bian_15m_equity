pub struct TradeMapper;
// use super::http_data::TradeRe;
use crate::actors::database::get_connect;
// use log::info;
use mysql::*;
use mysql::prelude::*;
use serde_json::Value;
// use super::db_data::Trade;


impl TradeMapper {
  // 插入数据
  pub fn insert_equity(equitys:Vec<Value>) -> bool {
    // 连接数据库
    let mut conn = get_connect();
    // let query_id = conn.exec_first(, params)

    let flag = conn.exec_batch(
      r"INSERT IGNORE INTO bian_15m_equity (name, equity, time, type)
      VALUES (:name, :equity, :time, :type)",
      equitys.iter().map(|p| params! {
        "name" => &p["name"],
        "equity" => &p["equity"],
        "time" => &p["time"],
        "type" => &p["type"],
      })
    );

    match flag {
      Ok(_c) => {
        println!("insert success!");
        return true;
      },
      Err(e) => {
        eprintln!("error:{}", e);
        return false;
      }
    }
  }


  // 插入bybit数据
  pub fn insert_bybit_equity(equitys:Vec<Value>) -> bool {
    // 连接数据库
    let mut conn = get_connect();
    // let query_id = conn.exec_first(, params)

    let flag = conn.exec_batch(
      r"INSERT IGNORE INTO total_bybit_equity (name, time, equity)
      VALUES (:name, :time, :equity)",
      equitys.iter().map(|p| params! {
        "name" => &p["name"],
        "time" => &p["time"],
        "equity" => &p["equity"],
      })
    );

    match flag {
      Ok(_c) => {
        println!("insert success!");
        return true;
      },
      Err(e) => {
        eprintln!("error:{}", e);
        return false;
      }
    }
  }
}










