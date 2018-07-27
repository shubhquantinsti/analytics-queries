// let csvToJson = require('convert-csv-to-json');
//
// let fileInputName = 'Data - Unit Details.csv';
// let fileOutputName = 'myOutputFile.json';
//
// csvToJson.generateJsonFileFromCsv(fileInputName,fileOutputName);

// const csv=require('csvtojson');
//
// const readStream=require('fs').createReadStream(file.csv);
//
// const writeStream=request.put('http://mysite.com/obj.json');
//
// readStream.pipe(csv()).pipe(writeStream);

var csvjson = require('csvjson');
var fs = require('fs');
var options = {
    delimiter : ',' , // optional
    quote     : '"' // optional
};
var file_data = fs.readFileSync('file.csv', { encoding : 'utf8'});
var result = csvjson.toObject(file_data, options);
//console.log(typeof(result));
var ff = JSON.stringify(result);
                                          //console.log(ff);
                                          fs.writeFile('jsonFile.json',ff,function(){
                                            console.log('Wrote');
                                           //var reader = fs.createReadStream('jsonFile.json');
                                           //var writer = fs.createWriteStream('dataQuantraUnit.csv');

                                          //reader.pipe(jsonexport()).pipe(writer);
                           });
