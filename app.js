// Required modules
const tmi = require('tmi.js');
const axios = require('axios');
const kafka = require('kafka-node');

// Categories which messages will be fetched
const categoriesIds = [
  ['509658'],
  ['26936', '509660', '509673', '1469308723', '509659'],
  ['488190', '498566', '499634', '29452'],
  ['417752', '515214', '509663'],
  ['509672', '509671', '509667', '272263131', '518203', '116747788'],
  [
    '512710',
    '1614555304',
    '515025',
    '516575',
    '32982',
    '32399',
    '511224',
    '33214',
    '491931',
    '263490',
  ],
  [
    '18122',
    '21779',
    '29595',
    '27471',
    '491487',
    '493597',
    '490100',
    '65632',
    '386821',
    '459931',
  ],
  [
    '102007682',
    '1745202732',
    '513143',
    '513181',
    '138585',
    '11989',
    '31339',
    '862021340',
    '31376',
    '2748',
  ],
];

// API requests for each category
const categoriesRequests = [];

// Channels to be fetched from each category
const channels = [];

// GET channel streaming specific category on twitch
const getChannelsStreamingCategory = async (category) => {
  try {
    const res = await axios.get(
      `https://api.twitch.tv/helix/streams?type=live&language=en&first=100&game_id=${category.join(
        '&game_id='
      )}`,
      {
        headers: {
          'Client-Id': 'g4dfdnc0soguw5es28kykivzwtpdw5',
          Authorization: 'Bearer wedmevykkgdf1xxx631g07dgdqlnnu',
        },
      }
    );
    return res;
  } catch (e) {
    console.log(e);
  }
};

// API call to get all channels for each category
const getChannels = async () => {
  for (let i = 0; i < categoriesIds.length; i++) {
    categoriesRequests.push(
      await getChannelsStreamingCategory(categoriesIds[i])
    );
  }
  try {
    const promises = await Promise.all(categoriesRequests);
    promises.forEach(({ data }) => {
      const res = data.data.map((a) => a.user_login);
      channels.push(res);
    });
  } catch (err) {
    console.log(err);
  }
};

// Start the client that will listen for messages
const startClient = async (producer) => {
  await getChannels();

  const client = new tmi.Client({
    connection: {
      secure: true,
      reconnect: true,
    },
    channels: channels[0],
  });

  client.connect();

  client.on('message', (channel, tags, message, self) => {
    if (tags.emotes == null && tags['message-type'] == 'chat' && !tags.mod) {
      payloads = [
        {
          topic: 'sparkTopic',
          messages: message,
          timestamp: Date.now(),
        },
        {
          topic: 'hdfsTopic',
          messages: message,
          timestamp: Date.now(),
        },
      ];

      producer.send(payloads, (err, data) => {
        console.log(`DATA: ${JSON.stringify(data)}`);
      });
    }
  });
};

const kafkaClient = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const kafkaProducer = new kafka.HighLevelProducer(kafkaClient);

kafkaProducer.on('ready', () => {
  startClient(kafkaProducer);
});

