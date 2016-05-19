#!/usr/bin/env node
//
// kstreaming.js
//
// Stream data from Tesla's streaming API to Apache Kafka
//
"use strict";
var request = require('request');
var teslams = require('teslams');
var util = require('util');
var JSONbig = require('json-bigint');
var KafkaRest = require('kafka-rest');

function argchecker( argv ) {
    //if (argv.kafka === true) throw 'Kafka REST Gateway URL is unspecified. Use -k, --kafka <broker_url> (i.e. "-k http://localhost:8082")';
    if (argv.topic === "") throw 'Kafka topic is unspecified. Use -t, --topic <topic>';
}

var usage = 'Usage: $0 -u <username> -p <password> [-sz] \n' +
    '   [--kafka <http://localhost:8082>] [--topic <mqtt_topic>] \n' +
    '   [--values <value list>] [--maxrpm <#num>] [--vehicle offset] [--naptime <#num_mins>]'; 

var s_url = 'https://streaming.vn.teslamotors.com/stream/';
var firstTime = true;
var last = 0; // datetime for checking request rates
var rpm = 0; // REST API Request Per Minute counter
var slast = 0; // datetime for checking streaming request rates
var srpm = 0; // Streaming URL Request Per Minute counter
var lastss = "init"; // last shift state
var ss = "init"; // shift state
var napmode = false; // flag for enabling pause to allow sleep to set in 
var sleepmode = false;
var napTimeoutId;
var sleepIntervalId;
// various instance counters to avoid multiple concurrent instances
var pcount = 0; 
var scount = 0;
var icount = 0;
var ncount = 0;

var argv = require('optimist')
    .usage(usage)
    .check(argchecker)
    .alias('u', 'username')
    .describe('u', 'Teslamotors.com login')
    .alias('p', 'password')
    .describe('p', 'Teslamotors.com password')
    .alias('s', 'silent')
    .describe('s', 'Silent mode: no output to console')
    .alias('z', 'zzz')
    .describe('z', 'enable sleep mode checking')
    .boolean(['s', 'z'])
    .alias('r', 'maxrpm')
    .describe('r', 'Maximum number of requests per minute')
    .default('r', 6)
    .describe('k', 'Publish to Apache Kafka')
    .alias('t', 'topic')
    .describe('t', 'MQTT publish topic')
    .alias('f', 'format')
    .describe('f', 'Publish message format (i.e. avro or binary')
    .default('f', 'avro')
    .alias('n', 'naptime')
    .describe('n', 'Number of minutes to nap')
    .default('n', 30)
    .alias('N', 'napcheck')
    .describe('N', 'Number of minutes between nap checks')
    .default('N', 1)
    .alias('O', 'vehicle')   
    .describe('O', 'Select the vehicle offset (i.e. 0 or 1) for accounts with multiple vehicles')
    .default('O', 0)
    .alias('S', 'sleepcheck')
    .describe('S', 'Number of minutes between sleep checks')
    .default('S', 1)
    .alias('v', 'values')
    .describe('v', 'List of values to collect')
    .default('v', 'speed,odometer,soc,elevation,est_heading,est_lat,est_lng,power,shift_state,range,est_range,heading')
    .alias('?', 'help')
    .describe('?', 'Print usage information');

// get credentials either from command line or ~/.teslams/config.json
var creds = require('./config.js').config(argv);

argv = argv.argv;
//convert time values from minutes to milliseconds
argv.napcheck *= 60000;
argv.sleepcheck *= 60000;
argv.naptime *= 60000;

if ( argv.help == true ) {
    console.log(usage);
    process.exit(1);
}


var kafka = new KafkaRest({ 'url': 'http://localhost:8082' });
if (!argv.topic) {
    console.log('No Kafka topic specified. Using "teslams"');
    argv.topic = 'teslams';
}
var kafka_topic = kafka.topic(argv.topic);
var key_schema = new KafkaRest.AvroSchema("string");
var value_schema = new KafkaRest.AvroSchema({
  "namespace": "teslams",
  "type": "record",
  "name": "streamdata",
  "fields": [
    {
      "name": "id_s",
      "type": "string"
    },
    {
      "name": "vehicle_id",
      "type": "string"
    },
    {
      "name": "timestamp",
      "type": "long"
    },
    {
      "name": "speed",
      "type": [
        "null",
        "int"
      ]
    },
    {
      "name": "odometer",
      "type": "float"
    },
    {
      "name": "soc",
      "type": "int"
    },
    {
      "name": "elevation",
      "type": "int"
    },
    {
      "name": "est_heading",
      "type": "int"
    },
    {
      "name": "est_lat",
      "type": "float"
    },
    {
      "name": "est_lng",
      "type": "float"
    },
    {
      "name": "power",
      "type": "int"
    },
    {
      "name": "shift_state",
      "type": [
        "null",
        {
          "type": "enum",
          "name": "SSType",
          "symbols": [
            "P",
            "D",
            "R"
          ]
        }
      ],
      "doc": "the current shift state of the car",
      "symbolDocs": {
        "P": "Parked",
        "D": "Drive",
        "R": "Reverse",
        "null": "Not Provided"
      }
    },
    {
      "name": "range",
      "type": "int"
    },
    {
      "name": "est_range",
      "type": "int"
    },
    {
      "name": "heading",
      "type": "int"
    }
  ]
});

function tsla_poll( vid, long_vid, token ) {    
    pcount++;
    if ( pcount > 1 ) {
        ulog('Too many pollers running, exiting this one');
        pcount = pcount - 1;
        return;
    }   
    if (napmode) {
        ulog('Info: car is napping, skipping tsla_poll()');
        pcount = pcount - 1;
        return;
    } 
    if (long_vid == undefined || token == undefined) {
        console.log('Error: undefined vehicle_id (' + long_vid +') or token (' + token +')');
        console.log('Exiting...');
        process.exit(1);
    } 
    var now = new Date().getTime();
    if ( now - slast < 60000) { // last streaming request was less than 1 minute ago
        ulog( srpm + ' of ' + argv.maxrpm + ' Stream requests since ' + slast);
        if ( now - slast < 0 ) {
            ulog('Warn: Clock moved backwards - Daylight Savings Time??');
            srpm = 0;
            slast = now;
        } else if (srpm > argv.maxrpm ) {
            ulog('Warn: throttling due to too many streaming requests per minute');
            setTimeout(function() { 
                tsla_poll( vid, long_vid, token );
            }, 60000);  // 1 minute
            pcount = pcount - 1;
            return;
        }   
    } else { // longer than a minute since last request
        srpm = 0;
        slast = now;
    }
    //napmode checking
    if ( argv.zzz == true && lastss == "" && ss == "") {
        //if not charging stop polling for 30 minutes
        rpm++;
        teslams.get_charge_state( vid, function (cs) { 
            if (cs.charging_state == 'Charging') {
                ulog('Info: car is charging, continuing to poll for data');
            } else {
                if (ncount == 0) {
                    ncount++;               
                    ulog('Info: 30 minute nap starts now');
                    napmode = true;
                    // 30 minutes of nap mode to let the car fall asleep        
                    napTimeoutId = setTimeout(function() { 
                        ncount = 0;
                        clearInterval(sleepIntervalId);
                        scount = 0;
                        napmode = false;
                        ss = 'nap';
                        lastss = 'nap';
                        initstream();
                    }, argv.naptime);    // 30 minute of nap time (default)
                } else {
                    ulog('Debug: (' + ncount + ') Nap timer is already running. Not starting another');
                }
                // check if sleep has set in every minute (default) 
                if (scount == 0) {
                    scount++;
                    sleepIntervalId = setInterval(function() {
                        if (napmode == true) {
                            rpm++;
                            // adding support for selecting which vehicle to poll from a multiple vehicle account 
                            teslams.all( { email: creds.username, password: creds.password }, function ( error, response, body ) {
                                var vdata, vehicles;
                                //check we got a valid JSON response from Tesla
                                try { 
                                    vdata = JSONbig.parse(body); 
                                } catch(err) { 
                                    console.log('Error: login failed, unable to parse vehicle data'); 
                                    process.exit(1);
                                }
                                //check we got an array of vehicles and get the right one using the (optionally) specified offset
                                if (!util.isArray(vdata.response)) {
                                    console.log('Expected an response in JSON array format from Tesla Motors');
                                    process.exit(1);
                                }
                                vehicles = vdata.response[argv.vehicle]; // cast to a string for BigInt protection????
                                if (vehicles === undefined) {
                                    console.log( 'No vehicle data returned for car number ' + argv.vehicle);
                                    process.exit(1);    
                                }
                            // end of new block added for multi-vehicle support                          
                            //teslams.vehicles( { email: creds.username, password: creds.password }, function ( vehicles ) {  
                                if ( typeof vehicles.state != undefined ) {
                                    ulog( 'Vehicle state is: ' + vehicles.state );
                                    if (vehicles.state == 'asleep' || vehicles.state == 'unknown') {
                                        ulog( 'Stopping nap mode since car is now in (' + vehicles.state + ') state' );
                                        clearTimeout(napTimeoutId);
                                        ncount = 0;
                                        clearInterval(sleepIntervalId);
                                        scount = 0;
                                        napmode = false;
                                        ss = 'sleep';
                                        lastss = 'sleep';
                                        initstream();
                                    }
                                } else {
                                    ulog( 'Nap checker: undefined vehicle state' );
                                }
                            });
                        }                   
                    }, argv.sleepcheck); // every 1 minute  (default)
                } else {
                    ulog('Debug: (' + scount + ') Sleep checker is already running. Not starting another');
                }
            }
        });
        // need to check again if nap mode flag had been changed above
        // [HJ] added some code to the .on('data') function below to detect and cancel 
        // nap mode if the car starts driving again.
        if (napmode == true) {
            ulog('Info: code just entered nap mode but we will start one last poll');
            // [HJ]
            // ulog('Info: code just entered nap mode canceling long poll');
            // pcount = pcount - 1;
            // return;
        } 
    }
    srpm++; //increment the number of streaming requests per minute
    request({'uri': s_url + long_vid +'/?values=' + argv.values,
            'method' : 'GET',
            'auth': {'user': creds.username,'pass': token},
            'timeout' : 125000 // a bit more than the expected 2 minute max long poll
            }, function( error, response, body) {
        if ( error ) { // HTTP Error
            ulog( 'Polling again because poll returned HTTP error:' + error );
            // put short delay to avoid infinite recursive loop and stack overflow
            setTimeout(function() {
                tsla_poll( vid, long_vid, token ); // poll again
            }, 10000);
            pcount = pcount - 1;
            return;
        } else if (response.statusCode == 200) { // HTTP OK
            if (body === undefined) {
                ulog('WARN: HTTP returned OK but body is undefined');
                setTimeout(function() {
                    tsla_poll( vid, long_vid, token ); // poll again
                }, 10000); //10 seconds
                pcount = pcount - 1;
                return;
            } else if (body === null) {
                ulog('WARN: HTTP returned OK but body is null');
                setTimeout(function() {
                    tsla_poll( vid, long_vid, token ); // poll again
                }, 10000); // 10 seconds
                pcount = pcount - 1;
                return;
            } else {
                ulog('Poll return HTTP OK and body is this:\n' + body);
                // put short delay to avoid infinite recursive loop and stack overflow
                setTimeout(function() {
                    tsla_poll( vid, long_vid, token ); // poll again
                }, 1000); // 1 second
                pcount = pcount - 1;
                return;
            }
        } else if ( response.statusCode == 401) { // HTTP AUTH Failed
            ulog('WARN: HTTP 401: Unauthorized - token has likely expired, reinitializing');
            setTimeout(function() {
                initstream();
            }, 5000); //5 seconds in milliseconds
            pcount = pcount - 1;
            return;
        } else if ( response.statusCode == 429) { // HTTP AUTH Failed - To Many Requests
            ulog('WARN: HTTP 429: Too Many Requests - Tesla is likely blocking or throttling your IP address');
            setTimeout(function() {
                initstream();
            }, 900000); //15 minutes in milliseconds
            pcount = pcount - 1;
            return;
        } else { // all other unexpected responses
            ulog('Unexpected problem with request:\n    Response status code = ' + response.statusCode + '  Error code = ' + error + '\n Polling again in 10 seconds...');
            // put short delay to avoid infinite recursive loop and stack overflow
            setTimeout(function() {
                tsla_poll( vid, long_vid, token ); // poll again
            }, 10000); // 10 seconds
            pcount = pcount - 1;
            return;
        }
    }).on('data', function(data) {
        // TODO: parse out shift_state field and assign to a global for better sleep checking
        var d, vals;              
		d = data.toString().trim();
		vals = d.split(/[,\n\r]/);
		//check we have a valid timestamp to avoid interpreting corrupt stream data             
		if ( isNaN(vals[0]) || vals[0] < 1340348400000) { //tesla epoch
			ulog('Bad timestamp (' + vals[0] + ')' );
		} else {
            if (argv.topic) {
                //publish to MQTT broker on specified topic
                var newchunk = d.replace(/[\n\r]/g, '');
                var array = newchunk.split(',');
                var streamdata = { 
                    id_s : vid.toString(),
                    vehicle_id : long_vid.toString(),
                    timestamp : Number( array[0] ), 
                    speed : (array[1] === "") ? null:Number( array[1] ), 
                    odometer : Number( array[2] ), 
                    soc : Number( array[3] ), 
                    elevation : Number( array[4] ), 
                    est_heading : Number( array[5] ), 
                    est_lat : Number( array[6] ), 
                    est_lng : Number( array[7] ), 
                    power : Number( array[8] ), 
                    shift_state : (array[9] === "") ? null:Number( array[9] ),
                    range : Number( array[10] ),
                    est_range : Number( array[11] ),
                    heading : Number( array[12] )
                };
                if (!argv.silent) { 
                    //ulog( 'streamdata is: ' + util.inspect(streamdata));
                }
                // Publish message
                    //streamdata.vehicle_id = streamdata.vehicle_id.toString();
                    try {
                        if (argv.format == "avro") {
                            console.log('publishing kafka avro data: ' + util.inspect(streamdata) );
                            kafka_topic.produce(value_schema, streamdata, function(err, res) {
                                if (err) {
                                    console.log('Kafka publishing error: ' + err );
                                } else {
                                    console.log('Kafka publishing response: ' + util.inspect(res));
                                }
                            }); // AVRO Publish    
                        } else {
                            // binary JSON kafka publish
                            console.log('publishing binary kafka data: ' + kafka_msg);
                            kafka_topic.produce(JSON.stringify(streamdata)); //Binary Publish
                        }
                    } catch (error) {
                        // failed to send, therefore stop publishing and log the error thrown
                        console.log('Error while publishing message to kafka broker: ' + error.toString());
                    }
                
            }  
            //after data is written and/or published deal with the napmode stuff
            lastss = ss; 
            ss = vals[9]; // TODO: fix hardcoded position for shift_state
            // [HJ] this section goes with the code above which allows one last poll
            // after entering nap mode. If this turns out to cause other problems
            // remove this nap cancel section AND switch back to disabling this
            // final poll
            if (napmode == true && ss != '') {
                //cancel nap mode           
                ulog('Info: canceling nap mode because shift_state is now (' + ss + ')'); 
                clearTimeout(napTimeoutId);
                ncount = 0;
                clearInterval(sleepIntervalId);
                scount = 0;
                napmode = false;
                ss = 'abort';
                lastss = 'abort';
                initstream();
            }
        }
    });     
}

function getAux() {
    // make absolutely sure we don't overwhelm the API
    var now = new Date().getTime();
    if ( now - last < 60000) { // last request was within the past minute
        ulog( 'getAux: ' + rpm + ' of ' + argv.maxrpm + ' REST requests since ' + last);
        if ( now - last < 0 ) {
            ulog('Warn: Clock moved backwards - Daylight Savings Time??');
            rpm = 0;
            last = now;
        } else if ( rpm > argv.maxrpm ) {
            ulog ('Throttling Auxiliary REST requests due to too much REST activity');
            return;
        }
    } else { // longer than a minute since last request
        rpm = 0;
        last = now;
    }
    // check if the car is napping
    if (napmode || sleepmode) {
        ulog('Info: car is napping or sleeping, skipping auxiliary REST data sample');
        //TODO add periodic /vehicles state check to see if nap mode should be cancelled because car is back online again
        return;
    } else {
        rpm = rpm + 2; // increase REST request counter by 2 for following requests
        ulog( 'getting charge state Aux data');
        teslams.get_charge_state( getAux.vid, function(data) {
            //var doc = { 'ts': new Date().getTime(), 'chargeState': data };
            if (argv.mqtt) { //add Kafka publisher
                //publish charge_state data
                data.timestamp = new Date().getTime();
                data.id_s = getAux.vid.toString();
                try {
                    //client.publish(argv.topic + '/' + getAux.vid + '/charge_state', JSON.stringify(data));
                } catch (error) {
                    // failed to send, therefore stop publishing and log the error thrown
                    console.log('Error while publishing charge_state message to mqtt broker: ' + error.toString());
                }
            }
        });
        ulog( 'getting climate state Aux data');
        teslams.get_climate_state( getAux.vid, function(data) {
            var ds = JSON.stringify(data), doc;
            if (ds.length > 2 && ds != JSON.stringify(getAux.climate)) {
                getAux.climate = data;
                doc = { 'ts': new Date().getTime(), 'climateState': data };
                if (argv.mqtt) { // add Kafka publisher
                    //publish climate_state data
                    data.timestamp = new Date().getTime();
                    data.id_s = getAux.vid.toString();
                    try {
                        //client.publish(argv.topic + '/' + getAux.vid + '/climate_state', JSON.stringify(data));
                    } catch (error) {
                        // failed to send, therefore stop publishing and log the error thrown
                        console.log('Error while publishing climate_state message to mqtt broker: ' + error.toString());
                    }
                }
            }    
        });
    }
}

function storeVehicles(vehicles) {
    var doc = { 'ts': new Date().getTime(), 'vehicles': vehicles };

    rpm = rpm + 2; // increment REST request counter for following 2 requests
    teslams.get_vehicle_state(vehicles.id, function(data) {
        ulog( util.inspect(data));
        if (argv.mqtt) { // add Kafka publish
            //publish vehicle_state data
            try {
                //client.publish(argv.topic + '/' + vehicles.id + '/vehicle_state', JSON.stringify(doc));
            } catch (error) {
                // failed to send, therefore stop publishing and log the error thrown
                console.log('Error while publishing vehicle_state message to mqtt broker: ' + error.toString());
            }
        }
    });
    teslams.get_gui_settings(vehicles.id, function(data) {
        ulog(util.inspect(data));
        doc = { 'ts': new Date().getTime(), 'guiSettings': data };
        if (argv.mqtt) { // add Kafka publish
            //publish gui_settings data
            try {
                //client.publish(argv.topic + '/' + vehicles.id + '/gui_settings', JSON.stringify(doc));
            } catch (error) {
                // failed to send, therefore stop publishing and log the error thrown
                console.log('Error while publishing gui_settings message to mqtt broker: ' + error.toString());
            }
        }
    });
}

// if we are storing into a database or publishing to mqtt we also want to
// - store/publish the vehicle data (once, after the first connection)
// - store/publish some other REST API data around climate and charging (every minute)
function initdb(vehicles) {
    storeVehicles(vehicles);
    getAux.vid = vehicles.id;
    setInterval(getAux, 60000); // also get non-streaming data every 60 seconds
}

function ulog( string ) {
    if (!argv.silent) {
        util.log( string );
    }
}

function initstream() {
    icount++;
    if ( icount > 1 ) {
        ulog('Debug: Too many initializers running, exiting this one');
        icount = icount - 1;
        return;
    }   
    if (napmode) {
        ulog('Info: car is napping, skipping initstream()');
        icount = icount - 1;
        return;
    } 
    // make absolutely sure we don't overwhelm the API
    var now = new Date().getTime();
    if ( now - last < 60000) { // last request was within the past minute
        ulog( rpm + ' of ' + argv.maxrpm + ' REST requests since ' + last);
        if ( now - last < 0 ) {
            ulog('Warn: Clock moved backwards - Daylight Savings Time??');
            rpm = 0;
            last = now;
        } else if (rpm > argv.maxrpm) { // throttle check
            util.log('Warn: throttling due to too many REST API requests');
            setTimeout(function() { 
                initstream(); 
            }, 60000); // 1 minute
            icount = icount - 1;
            return;
        }       
    } else { // longer than a minute since last request
        last = now;
        rpm = 0; // reset the REST API request counter
    }
    rpm++; // increment the REST API request counter
    // adding support for selecting which vehicle to poll from a multiple vehicle account 
    teslams.all( { email: creds.username, password: creds.password }, function ( error, response, body ) {
        var vdata, vehicles;
        //check we got a valid JSON response from Tesla
        try { 
            vdata = JSONbig.parse(body); 
        } catch(err) { 
            ulog('Error: unable to parse vehicle data response as JSON, login failed. Trying again.'); 
            setTimeout(function() { 
                initstream(); 
            }, 10000); // 10 second
            icount = icount - 1;
            return;
            //process.exit(1);
        }
        //check we got an array of vehicles and get the right one using the (optionally) specified offset
        if (!util.isArray(vdata.response)) {
            ulog('Error: expecting an array if vehicle data from Tesla but got this:');
            util.log( vdata.response );
            process.exit(1);
        }
        // use the vehicle offset from the command line (if specified) to identify the right car in case of multi-car account
        vehicles = vdata.response[argv.vehicle];
        if (vehicles === undefined) {
            ulog('Error: No vehicle data returned for car number ' + argv.vehicle);
            process.exit(1);    
        }
    // end of new block added for multi-vehicle support
    // teslams.vehicles( { email: creds.username, password: creds.password }, function ( vehicles ) {  
        if ( typeof vehicles == "undefined" ) {
            console.log('Error: undefined response to vehicles request' );
            console.log('Exiting...');
            process.exit(1);
        }
        if (vehicles.state == undefined) {
            ulog( util.inspect( vehicles) ); // teslams.vehicles call could return and error string
        }
        if (argv.zzz && vehicles.state != 'online') { //respect sleep mode
            var timeDelta = Math.floor(argv.napcheck / 60000) + ' minutes';
            if (argv.napcheck % 60000 != 0) {
                timeDelta += ' ' + Math.floor((argv.napcheck % 60000) / 1000) + ' seconds';
            }
            ulog('Info: car is in (' + vehicles.state + ') state, will check again in ' + timeDelta);
            napmode = true;
            // wait for 1 minute (default) and check again if car is asleep
            setTimeout(function() { 
                napmode = false;
                sleepmode = true;
                initstream();
            }, argv.napcheck); // 1 minute (default)
            icount = icount - 1;
            return;     
        } else if ( typeof vehicles.tokens == "undefined" || vehicles.tokens[0] == undefined ) {
            ulog('Info: car is in (' + vehicles.state + ') state, calling /charge_state to reveal the tokens');
            rpm++;  // increment the REST API request counter           
            teslams.get_charge_state( vehicles.id, function( resp ) {
                if ( resp.charging_state != undefined ) {
                    // returned valid response so re-initialize right away
                    ulog('Debug: charge_state request succeeded (' + resp.charging_state + '). \n  Reinitializing...');
                    setTimeout(function() { 
                        initstream(); 
                    }, 1000); // 1 second
                    icount = icount - 1;
                    return;
                } else {
                    ulog('Warn: waking up with charge_state request failed.\n  Waiting 30 secs and then reinitializing...');
                    // charge_state failed. wait 30 seconds before trying again to reinitialize
                    // no need to set napmode = true because we are trying to wake up anyway
                    setTimeout(function() { 
                        initstream(); 
                    }, 30000);   // 30 seconds    
                    icount = icount - 1;
                    return;       
                } 
            }); 
        } else { // this is the valid condition so we have the required tokens and ids
            sleepmode = false;
            if (firstTime) {    // initialize only once
                firstTime = false;
                initdb(vehicles);
            }
            tsla_poll( vehicles.id, vehicles.vehicle_id, vehicles.tokens[0] );
            icount = icount - 1;
            return;
        }
    }); 
}

// this is the main part of this program
// call the REST API in order get login and get the id, vehicle_id, and streaming password token
ulog('timestamp,' + argv.values);
initstream();
