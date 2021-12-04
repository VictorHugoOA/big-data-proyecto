const fs = require('fs');
const csvtojson = require('csvtojson');
const mongo = require('mongodb');
const { getSystemErrorMap } = require('util');

const MB_TO_READ = 300*1024*1024;

const url = "mongodb://localhost:27017/BDProyecto";

function roughSizeOfObject( object ) {

    var objectList = [];
    var stack = [ object ];
    var bytes = 0;

    while ( stack.length ) {
        var value = stack.pop();

        if ( typeof value === 'boolean' ) {
            bytes += 4;
        }
        else if ( typeof value === 'string' ) {
            bytes += value.length * 2;
        }
        else if ( typeof value === 'number' ) {
            bytes += 8;
        }
        else if
        (
            typeof value === 'object'
            && objectList.indexOf( value ) === -1
        )
        {
            objectList.push( value );

            for( var i in value ) {
                stack.push( value[ i ] );
            }
        }
    }
    return bytes;
}

async function main(args){
    console.log(args);
    let dbConn;

    if(args.length < 1 || args.length > 1){
        console.error("You must provide only 1 argument to the program");
        console.error("node main.js path-to-file");
        return;
    }

    if(!fs.existsSync(args[0])){
        console.error("Sorry, the file you provided doesn't exists");
        console.error("Error: file doesn't exists");
        return;
    }



    await mongo.MongoClient.connect(url, {
        useUnifiedTopology: true
    }).then((client) => {
        console.log('DB Connected');
        dbConn = client.db();
    }).catch((error) => {
        console.error(`DB Connection Error ${error.message}`);
        return;
    });
    let arrayToInsert = [];
    let totalMBRead = 0;
    const collection = dbConn.collection('bombardeos');
    csvtojson().fromFile(args[0]).subscribe((json, lineNumber) => {
        totalMBRead += roughSizeOfObject(json);
        if(totalMBRead >= MB_TO_READ){
            console.log('Data imported succesfully');
            process.exit();
            return;
        }
        collection.insertOne(json, (err, result) => {
            if(err) console.error(err);
        });
    });
}

let arrayArgs = [...process.argv];
arrayArgs.splice(0, 2);
main(arrayArgs);