var fs = require('fs');
var jsonexport = require('jsonexport');
const mongoose = require('mongoose');
var extend = require('extend');
const Schema = mongoose.Schema;
mongoose.Promise = global.Promise;
var merge = require('lodash.merge')

mongoose.connect('mongodb://localhost/first');

mongoose.connection.once('open',function(){
  console.log('Connection has been made...');
}).on('error',function(error){
    console.log('Connection error',error);
});

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

collQuiz.aggregate(
                [
                     {$match : { 'data.isOptionCorrect' : false }},
                     {$group: {_id: {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId'},notuniqueUserID: {$sum:1}}},
                     {$project : {"count_wrong_total": "$notuniqueUserID"}},
                     {$sort : {_id : 1}}
                 ],function(err,data){
                   //console.log(data);
                   json1 = data;
                   //console.log(json1.length);

                   collQuiz.aggregate(
                                   [
                                        {$match : { 'data.isOptionCorrect' : true }},
                                        {$group: {_id: {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId'},uniqueUserID: {$addToSet:'$userId'}}},
                                        {$project : {"count_correct_unique": {$size: "$uniqueUserID"}} },
                                        {$sort : {_id : 1}}
                                    ],function(err,data){
                                      json2 = data;
                                      //console.log(json2.length);
                                      //var json3 = joinObjects(json1, json2);
                                      //extend(true,json1, json2 );
                                      //console.log(json1.length);
                                      //var result = Object.assign({},json2, json1);
                                    //  console.log(json1[3]._id);
                                      var res = [];
                                      for (var i = 0; i < json1.length; i++) {
                                        for (var j = 0; j < json2.length; j++) {
                                            if(JSON.stringify(json1[i]._id) == JSON.stringify(json2[j]._id)){
                                              //res.push(json1[i]._id ,json1[i].count_wrong_total ,json2[j].count_correct_unique);
                                              res.push({_id: json1[i]._id, 'wrong' : json1[i].count_wrong_total, 'correct' : json2[j].count_correct_unique});
                                              break;
                                            }
                                        }
                                      }
                                      var ff = JSON.stringify(res);
                                      //console.log(ff);
                                      fs.writeFile('jsonFile.json',ff,function(){console.log('Wrote');
                                       var reader = fs.createReadStream('jsonFile.json');
                                       var writer = fs.createWriteStream('dataQuantraQuiz.csv');

                                      reader.pipe(jsonexport()).pipe(writer);

                                    });


                                    });

                 });
