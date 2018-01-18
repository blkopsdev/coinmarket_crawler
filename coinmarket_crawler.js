//include required node packages
var request = require('request');
var cheerio = require('cheerio');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;

//mongodb server url
var url = "mongodb://crypto_backend:Crypto*backend@ds129343-a0.mlab.com:29343,ds129343-a1.mlab.com:29343/heroku_ll9nqvpn?replicaSet=rs-ds129343";

//all cryptocurrencies api url
var all_coin_symbols_json_url = 'https://files.coinmarketcap.com/generated/search/quick_search.json';

//mongo connection
var dbo;
MongoClient.connect(url, function(err, db) {

    if (err) throw err;

    dbo = db.db('heroku_ll9nqvpn');

    dbo.collection('CoinMarketData_Staging').remove({});
    console.log('staging data removed!');
});

//get all coin list
request(all_coin_symbols_json_url, function (err, response, body) {

    if(err) throw err;

    if(response.statusCode !== 200) {

        console.log('Invalid status code: '+response.statusCode);

    }

    //coin symbol list
    var symbol_list = JSON.parse(body);
    //async function to get merket data per coin
    var itemsProcessed = 0;

    async.each(symbol_list, function (elem, callback) {
        
        var coin_name = elem.slug;
        var symbol = elem.symbol;
        var market_url = 'https://coinmarketcap.com/currencies/' + coin_name + '/';

        request(market_url, function(err, response, body) {

            if (!err) {

                var $ = cheerio.load(body);

                //crawl market data from the table html
                var markets_data = $("#markets-table tbody tr").map((i, element) => ({
                    symbol: symbol,
                    markets_data: {

                        source: $(element).find('td:nth-child(2)').text().trim(),

                        source_url: $(element).find('td:nth-child(3) a').attr('href'),

                        pair: $(element).find('td:nth-child(3)').text().trim().replace(symbol, '').replace('/', ''),

                        volume_h: $(element).find('td:nth-child(4)').text().replace('*', '').replace(/\n/g, '').trim(),

                        price: $(element).find('td:nth-child(5)').text().replace('*', '').replace(/\n/g, '').trim(),

                        volume_per: $(element).find('td:nth-child(5)').text().replace('*', '').replace(/\n/g, '').trim(),

                    },
                })).get()

                //insert market data to collection per coin
                dbo.collection('CoinMarketData_Staging').insertMany(markets_data, function(err, res) {
                    itemsProcessed ++;
                    console.log(itemsProcessed);
                    if(!err) {
                        console.log(coin_name + " documents inserted");
                        if (itemsProcessed == symbol_list.length) {
                            console.log("loop finished!");
                            mongodb_collection_process();
                        }
                        callback();

                    } else {
                        callback(err);
                    }

                });

            } else {
                itemsProcessed ++;
                callback(err);

            }
            
        });

    }, function (err) {

        if( err ) {

            console.log('Failed to insert');

        } else {

            console.log('Insert successfully');

        }

    });

});

function mongodb_collection_process() {
    console.log('loop finished!');
    dbo.collection("CoinMarketData").drop(function(err, delOK) {
        if (err) throw err;
        if (delOK) {
            console.log('live db droped');
            dbo.collection('CoinMarketData_Staging').rename('CoinMarketData', function(err, renameok){
                if (err) throw err;
                if (renameok) console.log('live db created');
            });
        }
    });
    
}