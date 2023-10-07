use std::collections::VecDeque;
use std::{collections::HashMap, fs, time::Duration};
// use chrono::Local;
use chrono:: Utc;
use log::{info, warn, error};
use serde_json::{Map, Value};
// use tokio::{sync::broadcast::{self, Receiver}};
use equity_15m_bybit_bian::adapters::binance::futures::http::actions::BinanceFuturesApi;
use equity_15m_bybit_bian::adapters::bybit::futures::http::actions::ByBitFuturesApi;
use equity_15m_bybit_bian::adapters::binance::papi::http::actions::BinancePapiApi;
use equity_15m_bybit_bian::base::ssh::SshClient;
use equity_15m_bybit_bian::base::wxbot::WxbotHttpClient;
use equity_15m_bybit_bian::actors::*;
// use test_alarm::models::http_data::*;

#[warn(unused_mut, unused_variables, dead_code)]
async fn real_time(
    symbols: &Vec<Value>,
    mut ssh_api: SshClient,
    wx_robot: WxbotHttpClient,
    ori_fund: f64,
) {
    //rece: &mut Receiver<&str>){
    info!("get ready for real time loop");
    let running = true;
    // let mut end = 6;

    // 每个品种的上一个trade_id
    let mut last_trade_ids: HashMap<String, u64> = HashMap::new();
    for symbol_v in symbols {
        let symbol = String::from(symbol_v.as_str().unwrap());
        let symbol = format!("{}USDT", symbol);
        last_trade_ids.insert(symbol, 0);
    }

    // 权益数据统计
    

    // 净值数据
    // let mut net_worth_histories: VecDeque<Value> = VecDeque::new();

    info!("begin real time loop");
    // 监控循环
    loop {
        info!("again");
        // json对象
        // let mut response: Map<String, Value> = Map::new();
        // let mut json_data: Map<String, Value> = Map::new();
        let mut map: Map<String, Value> = Map::new();
        let mut equity_histories: VecDeque<Value> = VecDeque::new();
        let mut equity_bybit_histories: VecDeque<Value> = VecDeque::new();
        
        
        let now = Utc::now();
        
        

        // 监控服务器状态
        info!("server process");
        let res = trade_mapper::TradeMapper::get_traders();
        if let Ok(binances) = res {

        for f_config in binances {
            let borrow = f_config.borrow;
            // let borrows: Vec<&str> = borrow.split('-').collect();
            if &f_config.tra_venue == "Binance" && &f_config.r#type == "Futures" {
                let mut equity_map: Map<String, Value> = Map::new();
                let date = format!("{}", now.format("%Y/%m/%d %H:%M:%S"));
        
            let binance_futures_api=BinanceFuturesApi::new(
                "https://fapi.binance.com",
                &f_config.api_key,
                &f_config.secret_key,
            );
            let name = f_config.tra_id;
            
            // let new_name:u64 = name.parse().unwrap();
            // let pro_id = binance_config.get("pro_id").unwrap().as_str().unwrap();

            if let Some(data) = binance_futures_api.account(None).await {
                let value: Value = serde_json::from_str(&data).unwrap();
                let assets = value.as_object().unwrap().get("assets")
            .unwrap().as_array().unwrap();
            let mut new_total_equity = 0.00;
            // let mut best_price = 0.00;
            for a in assets {
                let obj = a.as_object().unwrap();
                let wallet_balance: f64 = obj.get("walletBalance").unwrap().as_str().unwrap().parse().unwrap();
                let symbol = obj.get("asset").unwrap().as_str().unwrap();
    
                if wallet_balance != 0.00 {
                    if borrow.len() != 0 {
                        let borrows: Vec<&str> = borrow.split('-').collect();
                        if symbol == borrows[0]{
                            continue;
                        }
                    }
                    
                    let cross_un_pnl: f64 = obj.get("crossUnPnl").unwrap().as_str().unwrap().parse().unwrap();
                    let pnl = cross_un_pnl + wallet_balance;
                    // new_total_balance += wallet_balance;
                    new_total_equity += pnl;
                }
            }
            // 权益
            // let new_total_equity_eth: f64 = ((new_total_equity / best_price) - 28.97086) * best_price;
            equity_map.insert(String::from("time"), Value::from(date));
            equity_map.insert(String::from("name"), Value::from(name));
            equity_map.insert(String::from("equity"), Value::from(new_total_equity));
            
            // equity_map.insert(String::from("prod_id"), Value::from(pro_id));
            equity_map.insert(String::from("type"), Value::from("Futures"));
            equity_histories.push_back(Value::from(equity_map));
            } else {
                error!("Can't get {} bian-futures-positions.", name);
                continue;     
            }
        }




        if &f_config.tra_venue == "Binance" && &f_config.r#type == "Papi"{
            let mut equity_map: Map<String, Value> = Map::new();
            let date = format!("{}", now.format("%Y/%m/%d %H:%M:%S"));
        let binance_papi_api=BinancePapiApi::new(
            "https://papi.binance.com",
            &f_config.api_key,
            &f_config.secret_key,
        );
        let name = f_config.tra_id;
        // let new_name:u64 = name.parse().unwrap();
        // let pro_id = binance_config.get("pro_id").unwrap().as_str().unwrap();
    
        if let Some(data) = binance_papi_api.account(None).await {
            let value: Value = serde_json::from_str(&data).unwrap();
            let assets = value.as_array().unwrap();
        let mut equity = 0.0;
    
    for p in assets {
        let obj = p.as_object().unwrap();
        let amt:f64 = obj.get("totalWalletBalance").unwrap().as_str().unwrap().parse().unwrap();
        if amt == 0.0 {
            continue;
        } else {
            let symbol = obj.get("asset").unwrap().as_str().unwrap();
            if borrow.len() != 0 {
                let borrows: Vec<&str> = borrow.split('-').collect();
                if symbol == borrows[0]{
                    continue;
                }
            }  
            
                let unrealied_um:f64 = obj.get("umUnrealizedPNL").unwrap().as_str().unwrap().parse().unwrap();
                let unrealied_cm:f64 = obj.get("cmUnrealizedPNL").unwrap().as_str().unwrap().parse().unwrap();
                let unrealied = unrealied_cm + unrealied_um;
                let total_equity = unrealied + amt;
                equity += total_equity;
            
        }
    
        
    }
            equity_map.insert(String::from("time"), Value::from(date));
            equity_map.insert(String::from("name"), Value::from(name));
                equity_map.insert(String::from("equity"), Value::from(equity));
        // equity_map.insert(String::from("prod_id"), Value::from(pro_id));
        equity_map.insert(String::from("type"), Value::from("Papi"));
        equity_histories.push_back(Value::from(equity_map));
        } else {
            error!("Can't get {} biance-papi-positions.", name);
            continue;
        }
    }

        

    
        

        if &f_config.tra_venue == "ByBit" && &f_config.r#type == "Futures"{
            let mut equity_bybit_map: Map<String, Value> = Map::new();
            let date = format!("{}", now.format("%Y/%m/%d %H:%M:%S"));
        let bybit_futures_api=ByBitFuturesApi::new(
            "https://api.bybit.com",
            &f_config.api_key,
            &f_config.secret_key,
        );
        let name = f_config.tra_id;
        // let new_name:u64 = name.parse().unwrap();
        // let pro_id = binance_config.get("pro_id").unwrap().as_str().unwrap();

        if let Some(data) = bybit_futures_api.get_account_overview(Some("UNIFIED")).await {
            let value: Value = serde_json::from_str(&data).unwrap();
            println!("value{}", value);
            let result = value.get("result").unwrap().as_object().unwrap();
            let list = result.get("list").unwrap().as_array().unwrap();
            for i in list{
                let obj = i.as_object().unwrap();
                let equity:f64 = obj.get("totalEquity").unwrap().as_str().unwrap().parse().unwrap();
                equity_bybit_map.insert(String::from("name"), Value::from(name));
                equity_bybit_map.insert(String::from("time"), Value::from(date.clone()));
                equity_bybit_map.insert(String::from("equity"), Value::from(equity));
                equity_bybit_map.insert(String::from("type"), Value::from("Futures"));
            }

            equity_histories.push_back(Value::from(equity_bybit_map));

             
        } else {
            error!("Can't get {} bybit-positions.", name);
            continue;
        }

    

}



    

    

}

        }


      let res = trade_mapper::TradeMapper::insert_equity(Vec::from(equity_histories.clone()));
      println!("插入权益数据{}", res);




        // 获取账户信息
        

        



        // 等待下次执行
        info!("waiting for next real time task...({})", 90000 * 10);
        tokio::time::delay_for(Duration::from_millis(90000 * 10)).await;
    }
}

#[warn(unused_mut, unused_variables)]
#[tokio::main]
async fn main() {
    // 日志
    log4rs::init_file("./log4rs.yaml", Default::default()).unwrap();

    init();
    // let time = format!("{}", Local::now().format("%Y/%m/%d %H:%M:%S"));

    // 测试用api
    // let api_key="JwYo1CffkOLqmv2sC3Qhe2Qu5GgzbeLVw2BxWB5HgK6tnmc8yGfkzLuDImBgDkXm";
    // let api_secret="7FtQARZqM2PDgIZ5plr3nwEVYBXXbvmSuvmpf6Viz9e7Cq2B87grRTG3VZQiEC5C";

    // 连接数据库
    // let config_db: Value =
    //     serde_json::from_str(&fs::read_to_string("./configs/database.json").unwrap()).unwrap();

    // 读取配置
    let config: Value = serde_json::from_str(
        &fs::read_to_string("./configs/total.json").expect("Unable to read file"),
    )
    .expect("Unable to parse");

    // 任务间通信信道
    // let (send, mut rece) = broadcast::channel(32);

    // 创建任务
    let real_time_handle = tokio::spawn(async move {
        // let mut futures_config: Map<String, Value> = Map::new();
        // let mut servers_config = Map::new();
        let server_config = config.get("Server").unwrap();
        let symbols = config.get("Symbols").unwrap().as_array().unwrap();
        let key = config.get("Alarm").unwrap().get("webhook").unwrap().as_str().unwrap();
        // info!("获取key");
        let mut wxbot = String::from("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=");
        wxbot.push_str(key);
        info!("wxbot  {}", wxbot);
        let wx_robot = WxbotHttpClient::new(&wxbot);
        info!("preparing...");

        // for s_config in server_config{
        //     let obj = s_config.as_object().unwrap(); 
        //     let host = obj.get("host").unwrap().as_str().unwrap();
        //     let port = obj.get("port").unwrap().as_str().unwrap();
        //     let username = obj.get("username").unwrap().as_str().unwrap();
        //     let password = obj.get("password").unwrap().as_str().unwrap();
        //     let root_path = obj.get("root_path").unwrap().as_str().unwrap();
        //     let root_name = obj.get("root_name").unwrap().as_str().unwrap();
        //     servers_config.insert(String::from("host"), Value::from(host));
        //     servers_config.insert(String::from("port"), Value::from(port));
        //     servers_config.insert(String::from("username"), Value::from(username));
        //     servers_config.insert(String::from("password"), Value::from(password));
        //     servers_config.insert(String::from("root_path"), Value::from(root_path));
        //     servers_config.insert(String::from("root_name"), Value::from(root_name));
        // }
        
        
        
        let ssh_api = SshClient::new(
            server_config.get("host").unwrap().as_str().unwrap(),
            server_config.get("port").unwrap().as_str().unwrap(),
            server_config.get("username").unwrap().as_str().unwrap(),
            server_config.get("password").unwrap().as_str().unwrap(),
            server_config.get("root_path").unwrap().as_str().unwrap(),
            server_config.get("root_name").unwrap().as_str().unwrap(),
        );
        

        
        // for f_config in binance_future_config{
        //     let obj = f_config.as_object().unwrap(); 
        //     let base_url = obj.get("base_url").unwrap().as_str().unwrap();
        //     let api_key = obj.get("api_key").unwrap().as_str().unwrap();
        //     let secret_key = obj.get("secret_key").unwrap().as_str().unwrap();
        //     futures_config.insert(String::from("base_url"), Value::from(base_url));
        //     futures_config.insert(String::from("api_key"), Value::from(api_key));
        //     futures_config.insert(String::from("secret_key"), Value::from(secret_key));
        // }

        info!("created ssh client");
        // let binance_futures_api=BinanceFuturesApi::new(
        //     binance_config
        //         .get("futures")
        //         .unwrap()
        //         .get("base_url")
        //         .unwrap()
        //         .as_str()
        //         .unwrap(),
        //     binance_config
        //         .get("futures")
        //         .unwrap()
        //         .get("api_key")
        //         .unwrap()
        //         .as_str()
        //         .unwrap(),
        //     binance_config
        //         .get("futures")
        //         .unwrap()
        //         .get("secret_key")
        //         .unwrap()
        //         .as_str()
        //         .unwrap(),
        // );

        
        info!("created http client");

            real_time(symbols, ssh_api, wx_robot, 500.0).await;
        
    });

    // 开始任务
    info!("alarm begin(binance account)");
    real_time_handle.await.unwrap();
    info!("alarm done");
}
