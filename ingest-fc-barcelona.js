var Twitter = require('twitter');
var elasticsearch = require('elasticsearch');

var twitterClient = new Twitter({
  consumer_key: '',
  consumer_secret: '',
  access_token_key: '',
  access_token_secret: ''
});

var elasticsearchClient = new elasticsearch.Client({
  host: ''
});

var q = "barcelona,fcbarcelona,fc-barcelona,barcelona-fc,messi,neymar,suarez";
twitterClient.stream('statuses/filter', {track: q},  function(stream){
  stream.on('data', function(tweet) {
    users = [];
    if (tweet.entities) {
      for (var j = 0; j < tweet.entities.user_mentions.length; j++) {
        users.push(tweet.entities.user_mentions[j].screen_name);
      }
    }

    console.log(tweet.text);

    if (tweet.retweeted_status) {
      if (users.indexOf(tweet.retweeted_status.user.screen_name) < 0) {
        users.push(tweet.retweeted_status.user.screen_name);
      }
    }

    if (users.length > 0) {
      var user = {
        id: tweet.user.id_str,
        user_screen_name: tweet.user.screen_name,
        user_name: tweet.user.name
      }
      console.log(user, users);
      postUserToES(user, users);
    }

  });

  stream.on('error', function(error) {
    console.log("ERROR:", error);
  });
});

function postUserToES(user, associatedUsers) {
  elasticsearchClient.update({
    id: user.id,
    index: 'fc_barcelona_users',
    type: 'users',
    body: {
      script: 'ctx._source.retweeted_users.plus(retweeted_users)',
      params: { retweeted_users: associatedUsers },
      upsert: {
        id: user.id,
        user_screen_name: user.user_screen_name,
        user_name: user.user_name,
        retweeted_users: associatedUsers
      }
    }
  }, function (error) {
    if (error) {
      console.trace(error);
    } else {
      console.log('User updated successfully');
    }
  });
}
