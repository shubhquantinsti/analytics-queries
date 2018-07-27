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
                totalDurartion : Number,
                percentage : Number,
                flag : Number,
                createdAt : Date,
                updatedAt : Date
            });

var collectionW = mongoose.model('quantravid',workSchema, 'quantravid');

collectionW.aggregate(
            [
                {$match : { 'config.isPlaying' : true }},
                {$group : { _id : {'Playback_rate' : '$config.playbackRate','Full_screne' : '$config.isFullscreen','subtitle' : '$config.subtitle','muted' : '$config.isMuted','playing' : '$config.isPlaying'},uniqueUserID: {$addToSet:'$userId'}}},
                {$project : {"count_correct_unique": {$size: "$uniqueUserID"}}},
                {$sort : {'count_correct_unique' : -1}}
            ],function(err,data){
                        //console.log(data);
                        var ff = JSON.stringify(data);
                        fs.writeFile('jsonf.json',ff,function(){
                          var reader = fs.createReadStream('jsonf.json');
                          var writer = fs.createWriteStream('dataquantra.csv');

                          reader.pipe(jsonexport()).pipe(writer);
                          console.log('Wrote');
                        });


                });
