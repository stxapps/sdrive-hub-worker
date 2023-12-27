import { Datastore } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import {
  FILE_LOG, FILE_INFO, BUCKET_INFO, FILE_WORK_LOG, HUB_BUCKET, BACKUP_BUCKET,
} from './const';

const datastore = new Datastore();
const storage = new Storage();

// All tables: FileLog, FileInfo, BucketInfo, FileWorkLog, Blacklist, Revocation
// FileLog: auto key, path, assoIssAddress, action, size, sizeChange, createDate
//   source of truth in sdrive-hub/drivers/GcDriver.js
// FileInfo: path (key), status, size, createDate, updateDate
// BucketInfo: address (key), assoIssAddress, nItems, size (Bytes), createDate, updateDate
// FileWorkLog: auto key, lastKeys, lastCreateDate, createDate


const data = {

};

export default data;
