const Storage = require("./storage.js");
const fs = require("fs");
var xrpl = require("xrpl");
var beginLedgerIndex = 50000000
var step = 10000;
var throttle = 0.5;
var publicServer = "wss://s1.ripple.com/" //RPC server


var ledgerRequestPayload = {
    "command": "ledger",
    "ledger_index": "validated"
}

async function getLedger(client, ledgerIndex) {
    var ledgerRequest = ledgerRequestPayload;
    ledgerRequest.ledger_index = ledgerIndex;
    const response = await client.request(ledgerRequest);
    return response.result;
  }

async function main() {
    var storage = new Storage();
    try
    {
        fs.unlinkSync("./storage.db");
    } catch(err)
    {
    }

    const client = new xrpl.Client(publicServer);
    db = storage.createDatabase();

  try {
    console.log("Adding Ledger History Storage");
    await client.connect();
    let currentLedger = beginLedgerIndex;
    let resp = await getLedger(client, 'validated');
    let latestLedger = resp.ledger_index;
    do{
        try{
            console.log('Processing Ledger Index: ' + currentLedger)
            let respLedgerTime = await getLedger(client, currentLedger);
            let ledgerDateTime = respLedgerTime.ledger.close_time_human;
            let rippleEpoch = respLedgerTime.ledger.close_time;
            storage.insert(db,currentLedger,rippleEpoch,ledgerDateTime);
        } catch(err)
        {
            
        }
        currentLedger = currentLedger + step;
        await new Promise((r) => setTimeout(r, throttle * 1000));
    } while(currentLedger <= latestLedger)

    console.log('Completed adding ledger records to the db');
  } catch (err) {
    console.log(err);
  } finally {
    await client.disconnect();
  }
}

main()