var Twitter = require('twitter');
var kafka = require('kafka-node');
var {twitterconfig} = require('./twitterconfig');

var client = new Twitter({
  consumer_key: twitterconfig.consumer_key,
  consumer_secret: twitterconfig.consumer_secret,
  access_token_key: twitterconfig.access_token_key,
  access_token_secret: twitterconfig.access_token_secret  
});

var Producer = kafka.Producer;
var kclient = new kafka.Client('129.150.80.92:2181');
var producer = new Producer(kclient);

var payloads = [
    { topic: 'bigdata003-topicdemo', messages: ''}
];

client.stream('statuses/filter', {track: 'oracle'},  function(stream) {
   
    stream.on('data', function(tweet) {
        payloads[0].messages = JSON.stringify(tweet);
        //console.log (payloads[0].messages);
        producer.send(payloads, function (err, data) {
            console.log("Sent to EventHub !");});
            console.log(data);
        });
    
        stream.on('error', function(error) {
        console.log(error);
        });
  });

 