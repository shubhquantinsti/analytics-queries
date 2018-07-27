var fs = require('fs');
var jsonexport = require('jsonexport');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
mongoose.Promise = global.Promise;

mongoose.connect('mongodb://localhost/first');

mongoose.connection.once('open',function(){
  console.log('Connection has been made...');
}).on('error',function(error){
    console.log('Connection error',error);
});

var workSchema = new Schema({
                userId : String,
                sectionId : String,
                unitId : String,
                courseId : String,
                eventType : String,
                flag : Number,
                createdAt : Date,
                updatedAt : Date
            });

var collectionW = mongoose.model('quantraunit',workSchema, 'quantraunit');


var quizSchema = new Schema({

                        userId : String,
                        sectionId : String,
                        unitId : String,
                        courseId : String,
                        eventType : String,
                        data : {
                          optionSelected : Number,
                          isOptionCorrect : Boolean
                        },
                        flag : Number,
                        createdAt : Date,
                        updatedAt : Date

                    });

var collQuiz = mongoose.model('quantraquiz',quizSchema, 'quantraquiz');

var json1;
var json2;
var condition;
var t = 0;

collectionW.aggregate(
                            [
                                {$sort : {'createdAt' : 1}},
                                {$group: {_id: {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId'},dates :{$push : '$createdAt'},eventTypes : {$push : '$eventType'},users : {$push : '$userId'}}},
                                {$project : {'dates': 1,'eventTypes':1,'users':1}},
                                {$sort : {_id:1,users:1}}
                            ], function(err,data){
                                                //console.log(data);
                                          json1 = data;
                                          collQuiz.aggregate(
                                 [
                                   {$group: {_id: {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId'}}},
                                   {$sort : {_id:1}}
                                 ],function(err,data){
                                           json2 = data;
                                           var res = [];
                                           //console.log(typeof(json1._id));
                                           for (var i = 0; i < json1.length; i++){
                                             //var t = 0;

                                             for (var k = 0; k < json2.length; k++){
                                               condition = false;
                                               var v1 = json1[i]._id.courseId + json1[i]._id.sectionId + json1[i]._id.unitId;
                                              var v2 = json2[k]._id.courseId + json2[k]._id.sectionId + json2[k]._id.unitId;
                                               if(v1 == v2){
                                                 condition = true;
                                                 break;
                                               }
                                             }

                                              if(condition){
                                                        //var t = 0;
                                                       for (var j = 0; j < json1[i].eventTypes.length - 1; j++){

                                                         if(json1[i].eventTypes[j] == 'Unit Started'){
                                                           if((json1[i].eventTypes[j+1] == 'NavigateAway' || json1[i].eventTypes[j+1] == 'Unit Ended') && (json1[i].users[j] == json1[i].users[j+1])){
                                                             t +=  (json1[i].dates[j+1] - json1[i].dates[j]);

                                                             res.push({_id: json1[i]._id, 'user' : json1[i].users[j] ,'total_time' : t});
                                                             t = 0;
                                                             j++;
                                                           }
                                                         }

                                                       }

                                                       // res.push({_id: json1[i]._id, 'total_time' : t});
                                                       // t = 0;
                                                }

                                           }
                                           //console.log(res);
                                           var ff = JSON.stringify(res);
                                           //console.log(ff);
                                           fs.writeFile('jsonFile.json',ff,function(){console.log('Wrote');
                                            var reader = fs.createReadStream('jsonFile.json');
                                            var writer = fs.createWriteStream('dataQuantraUnit.csv');

                                           reader.pipe(jsonexport()).pipe(writer);
                            });
                 });
              });
