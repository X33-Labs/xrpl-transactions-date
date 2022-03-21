const Storage = require("./storage.js");
const fs = require("fs");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
var xrpl = require("xrpl");

//************CHANGE THESE VARIABLES ***************
var publicServer = "wss://s1.ripple.com/" //RPC server
var startDate = '2022-01-01' //Starting Date YYYY-MM-DD
var endDate = '2022-01-06' //Ending Date YYYY-MM-DD (Leave Blank if you want to search up to the latest ledger)
var account = ''
var throttle = 0.5
//************CHANGE THESE VARIABLES ***************


const csvWriter = createCsvWriter({
    path: "output.csv",
    header: [
      { id: "date", title: "date" },
      { id: "ledger_index", title: "ledger_index" },
      { id: "direction", title: "direction" },
      { id: "type", title: "type" },
      { id: "currency", title: "currency" },
      { id: "currency_issuer", title: "currency_issuer" },
      { id: "amount", title: "amount" },
      { id: "tx_hash", title: "tx_hash" },
      { id: "offer_create_msg", title: "offer_create_msg" }
    ],
  });


  var data = {
    values: [
    ]
  }

var ledgerRequestPayload = {
    "command": "ledger",
    "ledger_index": "validated"
}

var transactionPayload = {
    "command": "account_tx",
    "account": account,
    "ledger_index_min": 0,
    "ledger_index_max": 0
  }

  async function getLedger(client, ledgerIndex) {
    var ledgerRequest = ledgerRequestPayload;
    ledgerRequest.ledger_index = ledgerIndex;
    const response = await client.request(ledgerRequest);
    return response.result;
  }

function convertDateToRippleEpoch(date)
{
    let comparisonDate = new Date(date);
    var rippleEpoch = (comparisonDate.getTime() / 1000) - 946684800;
    return rippleEpoch;
}

function convertRippleEpochToDate(epoch)
{
    var d = new Date(0);
    d.setUTCSeconds(epoch + 946684800);
    return d.toISOString();
}

async function getAccountTransactions(client, marker, min, max) {
    const request = transactionPayload
    if (marker != undefined) {
      request.marker = marker;
    }
    request.ledger_index_min = min;
    request.ledger_index_max = max;
    const response = await client.request(request);
    return response.result;
  }

  function ProcessTransactions(transactions) {
    for (let i = 0; i < transactions.length; i++) {
        try{

        let direction = '';
        let currency = '';
        let amount = 0;
        let offer_create_msg = '';
        let currency_issuer = '';
        if(transactions[i].tx.Account == account)
        {
            direction = 'sent';
        } else {
            direction = 'received';
        }

        if(transactions[i].tx.TransactionType === "OfferCreate")
        {
            if(typeof transactions[i].tx.TakerGets == 'object')
            {
                currency_issuer = transactions[i].tx.TakerGets.issuer;
                if(transactions[i].tx.TakerGets.currency.length === 3)
                {
                    //standard currency code
                    currency = transactions[i].tx.TakerGets.currency;
                } else {
                    //non-standard, convert from hex
                    currency = xrpl.convertHexToString(transactions[i].tx.TakerGets.currency)
                }
                offer_create_msg = 'selling ' + transactions[i].tx.TakerGets.value + ' ' + currency + ' for ' + parseFloat((transactions[i].tx.TakerPays) / 1000000).toFixed(2) + ' XRP'
            } else {
                currency_issuer = transactions[i].tx.TakerPays.issuer;
                if(transactions[i].tx.TakerPays.currency.length === 3)
                {
                    //standard currency code
                    currency = transactions[i].tx.TakerPays.currency;
                } else {
                    //non-standard, convert from hex
                    currency = xrpl.convertHexToString(transactions[i].tx.TakerPays.currency)
                }
                offer_create_msg = 'buying ' + transactions[i].tx.TakerPays.value + ' ' + currency + ' for ' + parseFloat((transactions[i].tx.TakerGets) / 1000000).toFixed(2) + ' XRP'
            }
        } else {
            if(typeof transactions[i].tx.Amount == 'object')
            {
                amount = parseFloat(transactions[i].tx.Amount.value);
                currency_issuer = transactions[i].tx.Amount.issuer;
                if(transactions[i].tx.Amount.currency.length === 3)
                {
                    //standard currency code
                    currency = transactions[i].tx.Amount.currency;
                } else {
                    //non-standard, convert from hex
                    currency = xrpl.convertHexToString(transactions[i].tx.Amount.currency)
                }
            } else {
                currency = "XRP";
                amount = parseFloat(transactions[i].tx.Amount) / 1000000;
            }
        }
        if (isNaN(amount)) {
            amount = 0;
          }
            data.values.push(
                {
                    date: convertRippleEpochToDate(transactions[i].tx.date), 
                    ledger_index: transactions[i].tx.ledger_index,
                    direction: direction,
                    type: transactions[i].tx.TransactionType,
                    currency: currency,
                    currency_issuer: currency_issuer,
                    amount: amount.toString(),
                    tx_hash: transactions[i].tx.hash,
                    offer_create_msg: offer_create_msg
                }
            );
        } catch(err)
        {
            console.log(err);
        }
    }
  }

async function search()
{
    let db;
    let storage = new Storage();
    db = storage.getInstance();
    const client = new xrpl.Client(publicServer);
    try{
        await client.connect();
        let startingRow = [];
        let endingRow = [];
        let endingEpoch = 0;
        let startingEpoch = convertDateToRippleEpoch(startDate);
        let latestLedger = 0;
        let currentLedger = 0;
        if(endDate != '')
        {
            endingEpoch = convertDateToRippleEpoch(endDate);
            startingRow = await storage.returnClosestIndex(db,startingEpoch,'Start');
            endingRow = await storage.returnClosestIndex(db,endingEpoch,'End');
            latestLedger = endingRow[0].ledger_index;
            currentLedger = startingRow[0].ledger_index;
        } else {
            startingRow = await storage.returnClosestIndex(db,startingEpoch,'Start');
            let resp = await getLedger(client, 'validated');
            endingEpoch = resp.ledger.close_time;
            latestLedger = resp.ledger.ledger_index;
            currentLedger = startingRow[0].ledger_index;
        }

        if(startingRow.length === 0)
        {
            throw 'Starting date not found in database';
        }

        if(endingRow.length === 0 && endDate != '')
        {
            throw 'Ending date not found in database';
        }

        let marker = undefined;
        let totalTransactions = 0;
        let accountTx = await getAccountTransactions(client, marker, currentLedger, latestLedger);
        totalTransactions = totalTransactions + accountTx.transactions.length;
        ProcessTransactions(accountTx.transactions);
        marker = accountTx.marker;
        console.log('Processing ' + totalTransactions + ' Total Transactions... ')
        while(marker != undefined)
        {
            await new Promise((r) => setTimeout(r, throttle * 1000));
            accountTx = await getAccountTransactions(client, marker, currentLedger, latestLedger);
            ProcessTransactions(accountTx.transactions);
            marker = accountTx.marker;
            totalTransactions = totalTransactions + accountTx.transactions.length;
            console.log('Processing ' + totalTransactions + ' Total Transactions... ')
        }
        csvWriter.writeRecords(data.values).then(() => console.log("The CSV file was written successfully"));
    } catch(err)
    {
        console.log(err);
    } finally
    {
        await client.disconnect();
    }
}

async function main() {
    try {
        if (fs.existsSync("./storage.db")) {
          console.log("Database found...Starting to process.");

          //DB found, start searching
           search();
        } else {
          console.log("Database not found. Please run add-storage.js first");
          return;
        }
      } catch (err) {
        console.error(err);
      }
}




main();