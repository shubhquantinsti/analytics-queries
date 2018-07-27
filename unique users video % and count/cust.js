var fs = require('fs');
var jsonexport = require('jsonexport');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
mongoose.Promise = global.Promise;

// var mongoose = require('mongoose')
// require('mongoose-double')(mongoose);
// var SchemaTypes = mongoose.Schema.Types;

mongoose.connect('mongodb://localhost/first');

mongoose.connection.once('open',function(){
  console.log('Connection has been made...');
}).on('error',function(error){
    console.log('Connection error',error);
});

var workSchema = new Schema({
                config : {
                  isFullscreen : Boolean,
                  playbackRate : Number,
                  isMuted : Boolean,
                  isPlaying : Boolean,
                  subtitle : String
                },
                userId : String,
                sectionId : String,
                unitId : String,
                courseId : String,
                eventType : String,
                currentTime : Number,
                //currentTime : { type: SchemaTypes.Double },
                totalDurartion : Number,
                //totalDurartion : { type: SchemaTypes.Double },
                percentage : Number,
                flag : Number,
                createdAt : Date,
                updatedAt : Date
            });

 var collectionW = mongoose.model('quantravid',workSchema, 'quantravid');
// var data1 = new collectionW({userId:"abcd"});
// data1.save(function(err,data){
//   console.log(data);
// });

// collectionW.aggregate(
//                     [
//                         {$match : {unitId: "1", courseId: "55"} } ,
//                         {$group : {_id : {'%_completed' : "$percentage"}, customers : {$sum : '$flag'} }},
//                         {$sort : {_id : 1}}
//                     ]
//                 ).then(function(result){
//                    var cont = '';
//                    cont += result.toString();
//                   // console.log(cont);
//                   fs.writeFile('write.txt',cont,function(){console.log('Wrote');});
//
//                 });


// collectionW.aggregate(
//                     [
//                         {$match : {unitId: "1", courseId: "55"} } ,
//                         {$group : {_id : {'%_completed' : "$percentage"}, customers : {$sum : '$flag'} }},
//                         {$sort : {_id : 1}}
//                     ], function(err,data){
//                       console.log(data);
//                       fs.writeFile('write.txt',JSON.stringify(data),function(){console.log('Wrote');});
//                     });


collectionW.aggregate(
                  [
                     {$match : {percentage : {$in : [10,20,30,40,50,60,70,80,90,100]}}},
                     {$group: {_id: {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId','%_complete':'$percentage'}, uniqueUserID: {$addToSet:'$userId'}}},
                     {$project : {"count": {$size: "$uniqueUserID"}} },
                     {$sort : {_id : 1}}
                  ],function(err,data){
                        console.log(data);
                        var ff = JSON.stringify(data);
                        fs.writeFile('jsonf.json',ff,function(){console.log('Wrote');});

                        var reader = fs.createReadStream('jsonf.json');
                        var writer = fs.createWriteStream('dataquantra.csv');

                        reader.pipe(jsonexport()).pipe(writer);
                });
