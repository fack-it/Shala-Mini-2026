
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const config = require('../config')

const databaseFolder = path.join(__dirname, 'database');

if (!fs.existsSync(databaseFolder)) {
  fs.mkdirSync(databaseFolder);
}

const settingsSchema = new mongoose.Schema({
  OWNER_NUMBER: { type: String, default: "94704227534" },
  MV_SIZE: { type: String, default: "0" },
  NAME: { type: String, default: "Shala Mini" },
  JID: { type: String, default: "" },
  SEEDR_MAIL: { type: String, default: "" },
  SEEDR_PASSWORD: { type: String, default: "" },
  LANG: { type: String, default: "SI" },
  SUDO: { type: [String], default: [] },
  JID_BLOCK: { type: [String], default: [] },
  ANTI_BAD: { type: [String], default: [] },
  MAX_SIZE: { type: Number, default: 500 },
  ANTI_CALL: { type: String, default: "false" },
  AUTO_READ_STATUS: { type: String, default: "false" },
  AUTO_BLOCK: { type: String, default: "false" },
  AUTO_STICKER: { type: String, default: "false" },
  AUTO_VOICE: { type: String, default: "false" },
  AUTO_REACT: { type: String, default: "false" },  
  CMD_ONLY_READ: { type: String, default: "true" },
  WORK_TYPE: { type: String, default: "private" },
  XNXX_BLOCK: { type: String, default: "true" },
  AUTO_MSG_READ: { type: String, default: "false" },
  AUTO_TYPING: { type: String, default: "false" },
  AUTO_RECORDING: { type: String, default: "false" },
  AUTO_WELCOME_LEAVE: { type: [String], default: [] },
  ANTI_LINK: { type: String, default: "false" },
  ANTI_BOT: { type: String, default: "false" },
  ALIVE: { type: String, default: "default" },
  PREFIX: { type: String, default: "." },
  CHAT_BOT: { type: String, default: "false" },
  ALLWAYS_OFFLINE: { type: String, default: "false" },
  MV_BLOCK: { type: String, default: "true" },
  BUTTON: { type: String, default: "false" },
  ACTION: { type: String, default: "delete" },
  ANTILINK_ACTION: { type: String, default: "delete" },
  VALUSE: { type: [String], default: [] },
LOGO: { 
  type: String, 
  default: "https://files.catbox.moe/guicod.jpg" 
},
MAINMENU: { 
  type: String, 
  default: "https://i.ibb.co/mV9Hthsh/temp-1762331502295.jpg" 
},
  GROUPMENU: { 
  type: String, 
  default: "https://i.ibb.co/mV9Hthsh/temp-1762331502295.jpg" 
},
  OWNERMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  CONVERTMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  AIMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  LOGOMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  DOWNMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  SEARCHMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  OTHERMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  MOVIEMENU: { 
  type: String, 
  default: "https://i.ibb.co/YBTJ7k4v/temp-1762328278526.jpg" 
},
  ANTI_DELETE: { type: String, default: "off" },
  AUTO_VOICE: { type: String, default: "false" },
  LEAVE_MSG: { type: String, default: "" },
  AUTO_STATUS_REACT: { type: String, default: "true" },
  CUSTOM_REACT: { type: String, default: "" },
  
});

const Settings = mongoose.model(config.DB_NAME, settingsSchema);

async function connectdb() {
  try {
    await mongoose.connect('oyage mongo db url eka danna methannata😁');

    console.log("Database connected 🛢️");

    const settingsCount = await Settings.countDocuments();
    if (settingsCount === 0) {
      await initializeSettings();
    }
  } catch (error) {
    console.error(" ├ Database connection error:", error);
  }
}

async function initializeSettings() {
  const settings = new Settings(); 
  await settings.save(); 
  console.log("Settings initialized ✅");
}

async function updateCMDStore(MsgID, CmdID) {
  try {
    const filePath = path.join(databaseFolder, 'data.json');
    const olds = fs.existsSync(filePath) ? await readJsonFile(filePath) : [];
    olds.push({ [MsgID]: CmdID });
    await writeJsonFile(filePath, olds);
    return true;
  } catch (e) {
    console.log("Error updating command store:", e);
    return false;
  }
}

async function isbtnID(MsgID) {
  try {
    const filePath = path.join(databaseFolder, 'data.json');
    const olds = fs.existsSync(filePath) ? await readJsonFile(filePath) : [];
    return olds.some(item => item[MsgID]); //
  } catch (e) {
    return false;
  }
}

async function getCMDStore(MsgID) {
  try {
    const filePath = path.join(databaseFolder, 'data.json');
    const olds = fs.existsSync(filePath) ? await readJsonFile(filePath) : [];
    const foundItem = olds.find(item => item[MsgID]);
    return foundItem ? foundItem[MsgID] : null; 
  } catch (e) {
    console.log("Error retrieving command store:", e);
    return null;
  }
}

async function input(setting, data) {
  const settings = await Settings.findOne(); 
  if (settings && setting in settings) {
    settings[setting] = data;
    await settings.save(); 
  }
}

async function get(setting) {
  const settings = await Settings.findOne();
  return settings ? settings[setting] : null; 
}

async function updb() {
  const settings = await Settings.findOne(); 
  Object.assign(config, settings.toObject());
  console.log("Database updated ✅");
}

async function updfb() {
  await resetSettings(); 
  console.log("Database reset and initialized ✅");
}

async function upresbtn() {
  await writeJsonFile(path.join(databaseFolder, 'data.json'), []);
  console.log(" ├ Command store reset ✅");
}

function getCmdForCmdId(CMD_ID_MAP, cmdId) {
  const result = CMD_ID_MAP.find(entry => entry.cmdId === cmdId);
  return result ? result.cmd : null;
}

async function resetSettings() {
  await Settings.deleteMany(); 
  await initializeSettings(); 
}

async function readJsonFile(filePath) {
  return new Promise((resolve, reject) => {
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(JSON.parse(data));
      }
    });
  });
}

async function getalls() {
  const settings = await Settings.findOne();
  return settings ? settings.toJSON() : null;
}

async function writeJsonFile(filePath, data) {
  return new Promise((resolve, reject) => {
    fs.writeFile(filePath, JSON.stringify(data, null, 2), (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(true);
      }
    });
  });
}

connectdb().catch(console.error);

module.exports = {
  updateCMDStore,
  isbtnID,
  getCMDStore,
  input,
  get,
  getalls,
  updb,
  updfb,
  upresbtn,
  getCmdForCmdId,
  connectdb,
};
























