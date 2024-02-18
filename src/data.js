import { Datastore, PropertyFilter, and } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import { FILE_LOG, FILE_INFO, BUCKET_INFO, FILE_WORK_LOG, DELETED } from './const';
import { isString } from './utils';

const datastore = new Datastore();
const storage = new Storage();

// Tables: FileLog, FileInfo, BucketInfo, FileWorkLog, Blacklist, Revocation
// FileLog: auto key, path, assoIssAddress, action, size, sizeChange, createDate
//   source of truth in sdrive-hub/drivers/GcDriver.js
// FileInfo: path (key), status, size, createDate, updateDate
// BucketInfo: address (key), assoIssAddress, nItems, size (Bytes), createDate,
//   updateDate
// FileWorkLog: auto key, lastKeys, lastCreateDate, createDate

const getFileLogs = async (createDate) => {
  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_LOG);
    query.filter(new PropertyFilter('createDate', '>=', createDate));
    query.order('createDate', { descending: false });
    query.limit(3200);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const logs = [];
    for (const entity of entities) {
      const log = {
        key: entity[datastore.KEY].name,
        path: entity.path,
        assoIssAddress: entity.assoIssAddress,
        action: entity.action,
        size: entity.size,
        sizeChange: entity.sizeChange,
        createDate: entity.createDate,
      };
      logs.push(log);
    }
    return logs;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const getLatestFileLogs = async () => {
  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_LOG);
    query.order('createDate', { descending: true });
    query.limit(100);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const logs = [];
    for (const entity of entities) {
      const log = { key: entity[datastore.KEY].name, createDate: entity.createDate };
      logs.push(log);
    }
    return logs;
  } catch (e) {
    await transaction.rollback();
    throw e;
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
      const log = { key: entity[datastore.KEY].name };
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

const getFileInfos = async (paths) => {
  const keys = [];
  for (const path of paths) {
    keys.push(datastore.key([FILE_INFO, path]));
  }

  const nKeys = 800, infos = [];
  for (let i = 0; i < keys.length; i += nKeys) {
    const selectedKeys = keys.slice(i, i + nKeys);

    const transaction = datastore.transaction({ readOnly: true });
    try {
      await transaction.run();
      const [entities] = await transaction.get(selectedKeys);

      await transaction.commit();

      for (const entity of entities) {
        const info = {
          path: entity[datastore.KEY].name,
          status: entity.status,
          size: entity.size,
          createDate: entity.createDate,
          updateDate: entity.updateDate,
        };
        infos.push(info);
      }
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }

  return infos;
};

const getAllFileInfos = async () => {
  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_INFO);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const infos = [];
    for (const entity of entities) {
      const info = {
        path: entity[datastore.KEY].name,
        status: entity.status,
        size: entity.size,
        createDate: entity.createDate,
        updateDate: entity.updateDate,
      };
      infos.push(info);
    }
    return infos;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

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
    ])); // Need Composite Index Configuration in index.yaml in sdrive-hub
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

const updateFileInfos = async (fileInfos) => {
  const entities = [];
  for (const fileInfo of fileInfos) {
    const entity = {
      key: datastore.key([FILE_INFO, fileInfo.path]),
      data: [
        { name: 'status', value: fileInfo.status },
        { name: 'size', value: fileInfo.size, excludeFromIndexes: true },
        { name: 'createDate', value: fileInfo.createDate },
        { name: 'updateDate', value: fileInfo.updateDate },
      ],
    };
    entities.push(entity);
  }

  const nEntities = 64;
  for (let i = 0; i < entities.length; i += nEntities) {
    const selectedEntities = entities.slice(i, i + nEntities);

    const transaction = datastore.transaction();
    try {
      await transaction.run();

      transaction.save(selectedEntities);
      await transaction.commit();
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
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

const getBucketInfos = async (addrs) => {
  const keys = [];
  for (const addr of addrs) {
    keys.push(datastore.key([BUCKET_INFO, addr]));
  }

  const nKeys = 800, infos = [];
  for (let i = 0; i < keys.length; i += nKeys) {
    const selectedKeys = keys.slice(i, i + nKeys);

    const transaction = datastore.transaction({ readOnly: true });
    try {
      await transaction.run();
      const [entities] = await transaction.get(selectedKeys);

      await transaction.commit();

      for (const entity of entities) {
        const info = {
          address: entity[datastore.KEY].name,
          assoIssAddress: entity.assoIssAddress,
          nItems: entity.nItems,
          size: entity.size,
          createDate: entity.createDate,
          updateDate: entity.updateDate,
        };
        infos.push(info);
      }
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }

  return infos;
};

const getAllBucketInfos = async () => {
  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(BUCKET_INFO);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const infos = [];
    for (const entity of entities) {
      const info = {
        address: entity[datastore.KEY].name,
        assoIssAddress: entity.assoIssAddress,
        nItems: entity.nItems,
        size: entity.size,
        createDate: entity.createDate,
        updateDate: entity.updateDate,
      };
      infos.push(info);
    }
    return infos;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const updateBucketInfos = async (bucketInfos) => {
  const entities = [];
  for (const bucketInfo of bucketInfos) {
    const entity = {
      key: datastore.key([BUCKET_INFO, bucketInfo.address]),
      data: [
        { name: 'assoIssAddress', value: bucketInfo.assoIssAddress },
        { name: 'nItems', value: bucketInfo.nItems },
        { name: 'size', value: bucketInfo.size },
        { name: 'createDate', value: bucketInfo.createDate },
        { name: 'updateDate', value: bucketInfo.updateDate },
      ],
    };
    entities.push(entity);
  }

  const nEntities = 64;
  for (let i = 0; i < entities.length; i += nEntities) {
    const selectedEntites = entities.slice(i, i + nEntities);

    const transaction = datastore.transaction();
    try {
      await transaction.run();

      transaction.save(selectedEntites);
      await transaction.commit();
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }
};

const getLatestFileWorkLog = async () => {
  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_WORK_LOG);
    query.order('lastCreateDate', { descending: true });
    query.limit(1);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    let log = null;
    for (const entity of entities) {
      let lastKeys = entity.lastKeys;
      if (isString(lastKeys)) lastKeys = lastKeys.split(',');
      if (!Array.isArray(lastKeys)) throw new Error(`Invalid lastKeys: ${lastKeys}`);

      log = {
        key: entity[datastore.KEY].id,
        lastKeys,
        lastCreateDate: entity.lastCreateDate,
        createDate: entity.createDate,
      };
    }
    return log;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const saveFileWorkLog = async (lastKeys, lastCreateDate) => {
  if (Array.isArray(lastKeys)) lastKeys = lastKeys.join(',');
  if (!isString(lastKeys)) throw new Error(`Invalid lastKeys: ${lastKeys}`);

  const logData = [
    { name: 'lastKeys', value: lastKeys, excludeFromIndexes: true },
    { name: 'lastCreateDate', value: lastCreateDate },
    { name: 'createDate', value: new Date() },
  ];
  await datastore.save({ key: datastore.key([FILE_WORK_LOG]), data: logData });
};

const listFiles = async (bucketName) => {
  const files = [];
  await new Promise((resolve, reject) => {
    const readable = storage.bucket(bucketName).getFilesStream({ autoPaginate: false });
    readable.on('error', (error) => {
      reject(error);
    });
    readable.on('data', (file) => {
      const { name: path, metadata } = file;
      const size = parseInt(metadata.size, 10);
      const createDate = new Date(metadata.timeCreated);
      const updateDate = new Date(metadata.updated);
      files.push({ path, size, createDate, updateDate });
    });
    readable.on('end', () => {
      resolve(files);
    });
  });
  return files;
};

const copyFile = async (bucketName, path, destBucketName) => {
  const bucket = storage.bucket(bucketName);
  const destBucket = storage.bucket(destBucketName);

  const bucketFile = bucket.file(path);
  await bucketFile.copy(destBucket, { predefinedAcl: 'private' });
};

const deleteFiles = async (bucketName, paths) => {
  const bucket = storage.bucket(bucketName);
  for (const path of paths) {
    const bucketFile = bucket.file(path);
    await bucketFile.delete();
  }
};

const data = {
  getFileLogs, getLatestFileLogs, getObsoleteFileLogs, deleteFileLogs, getFileInfos,
  getAllFileInfos, getDeletedFileInfos, updateFileInfos, deleteFileInfos,
  getBucketInfos, getAllBucketInfos, updateBucketInfos, getLatestFileWorkLog,
  saveFileWorkLog, listFiles, copyFile, deleteFiles,
};

export default data;
