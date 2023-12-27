import { Datastore, PropertyFilter, and } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import { FILE_LOG, FILE_INFO, BACKUP_BUCKET, DELETED } from './const';
import { randomString } from './utils';

const datastore = new Datastore();
const storage = new Storage();

const getDeletedFileInfos = async () => {
  const dt = Date.now() - (31 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_INFO);
    query.filter(and([
      new PropertyFilter('status', '=', DELETED),
      new PropertyFilter('updateDate', '<', date),
    ]));
    query.order('updateDate', { descending: false });
    query.limit(800);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const infos = [];
    for (const entity of entities) {
      const info = {
        path: entity[datastore.KEY].name,
      };
      infos.push(info);
    }
    return infos;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const deleteFiles = async (bucket, paths) => {
  for (const path of paths) {
    const bucketFile = bucket.file(path);
    await bucketFile.delete();
  }
};

const deleteFileInfos = async (fileInfos) => {
  const keys = [];
  for (const fileInfo of fileInfos) {
    keys.push(datastore.key([FILE_INFO, fileInfo.path]));
  }

  const nKeys = 64;
  for (let i = 0; i < keys.length; i += nKeys) {
    const selectedKeys = keys.slice(i, i + nKeys);

    const transaction = datastore.transaction();
    try {
      await transaction.run();

      transaction.delete(selectedKeys);
      await transaction.commit();
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }
};

const getObsoleteFileLogs = async () => {
  const dt = Date.now() - (31 * 24 * 60 * 60 * 1000);
  const date = new Date(dt);

  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_LOG);
    query.filter(new PropertyFilter('createDate', '<', date));
    query.order('createDate', { descending: false });
    query.limit(800);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const logs = [];
    for (const entity of entities) {
      const log = { key: entity[datastore.KEY].id };
      logs.push(log);
    }
    return logs;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const deleteFileLogs = async (fileLogs) => {
  const keys = [];
  for (const fileLog of fileLogs) {
    keys.push(datastore.key([FILE_LOG, fileLog.key]));
  }

  const nKeys = 64;
  for (let i = 0; i < keys.length; i += nKeys) {
    const selectedKeys = keys.slice(i, i + nKeys);

    const transaction = datastore.transaction();
    try {
      await transaction.run();

      transaction.delete(selectedKeys);
      await transaction.commit();
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }
};

const cleanUp = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) cleanUp starts on ${startDate.toISOString()}`);

  const backupBucket = storage.bucket(BACKUP_BUCKET);

  // Backup Storage and FileInfo
  const fileInfos = await getDeletedFileInfos();
  const paths = fileInfos.map(fileInfo => fileInfo.path);
  await deleteFiles(backupBucket, paths);
  await deleteFileInfos(fileInfos);

  // FileLog
  const fileLogs = await getObsoleteFileLogs();
  await deleteFileLogs(fileLogs);

  // BucketInfo: Do manually on GCloud Console for now.

  // FileWorkLog: Do manually on GCloud Console for now.

  console.log(`(${logKey}) CleanUp finishes on ${(new Date()).toISOString()}.`);
};

cleanUp();
