import { Datastore, PropertyFilter } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import {
  FILE_LOG, FILE_INFO, BUCKET_INFO, FILE_WORK_LOG, HUB_BUCKET, BACKUP_BUCKET, ACTIVE,
  DELETED,
} from './const';
import { isObject, isString, randomString, extractPath } from './utils';

const datastore = new Datastore();
const storage = new Storage();

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
      const log = { key: entity[datastore.KEY].id, createDate: entity.createDate };
      logs.push(log);
    }
    return logs;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const getNewerFileLogs = async (createDate) => {
  const transaction = datastore.transaction({ readOnly: true });
  try {
    await transaction.run();

    const query = datastore.createQuery(FILE_LOG);
    query.filter(new PropertyFilter('createDate', '>=', createDate));
    query.limit(800);
    const [entities] = await transaction.runQuery(query);

    await transaction.commit();

    const logs = [];
    for (const entity of entities) {
      const log = { key: entity[datastore.KEY].id, createDate: entity.createDate };
      logs.push(log);
    }
    return logs;
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const getFileInfos = async () => {
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

const getBucketInfos = async () => {
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

const listFiles = async (bucket) => {
  const files = [];
  await new Promise((resolve, reject) => {
    const readable = bucket.getFilesStream({ autoPaginate: false });
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

const rework = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) rework starts on ${startDate.toISOString()}`);

  // Datastore
  const latestFileLogs = await getLatestFileLogs();
  const fileInfos = await getFileInfos();
  const bucketInfos = await getBucketInfos();

  const fileInfosPerPath = {}, bucketInfosPerAddress = {};
  for (const fileInfo of fileInfos) {
    fileInfosPerPath[fileInfo.path] = fileInfo;
  }
  for (const bucketInfo of bucketInfos) {
    bucketInfosPerAddress[bucketInfo.address] = bucketInfo;
  }

  // Storage
  const hubBucket = storage.bucket(HUB_BUCKET);
  const backupBucket = storage.bucket(BACKUP_BUCKET);

  const hubFiles = await listFiles(hubBucket);
  const backupFiles = await listFiles(backupBucket);

  const hubFilesPerPath = {}, backupFilesPerPath = {};
  for (const hubFile of hubFiles) {
    hubFilesPerPath[hubFile.path] = hubFile;
  }
  for (const backupFile of backupFiles) {
    backupFilesPerPath[backupFile.path] = backupFile;
  }

  // Process: From Hub -> update Backup, FileInfo, and BucketInfo
  const udtdFileInfos = [], udtdBifsPerAddr = {}, udtdBucketInfos = [];
  for (const hubFile of hubFiles) {
    const backupFile = backupFilesPerPath[hubFile.path];
    if (!isObject(backupFile) || hubFile.updateDate > backupFile.updateDate) {
      const bucketFile = hubBucket.file(hubFile.path);
      await bucketFile.copy(backupBucket, { predefinedAcl: 'private' });
    }

    let doUpdate = false, udtdFileInfo;
    const fileInfo = fileInfosPerPath[hubFile.path];
    if (isObject(fileInfo)) {
      if (fileInfo.status !== ACTIVE || hubFile.size !== fileInfo.size) {
        [doUpdate, udtdFileInfo] = [true, { ...fileInfo }];
      }
    } else {
      const [path, createDate] = [hubFile.path, hubFile.createDate];
      [doUpdate, udtdFileInfo] = [true, { path, createDate }];
    }
    if (doUpdate) {
      udtdFileInfo.status = ACTIVE;
      udtdFileInfo.size = hubFile.size;
      udtdFileInfo.updateDate = hubFile.updateDate;
      udtdFileInfos.push(udtdFileInfo);
    }

    const { address } = extractPath(hubFile.path);
    if (!isObject(udtdBifsPerAddr[address])) {
      udtdBifsPerAddr[address] = {
        nItems: 0, size: 0, createDate: null, updateDate: null,
      };
    }
    udtdBifsPerAddr[address].nItems += 1;
    udtdBifsPerAddr[address].size += hubFile.size;
    if (
      !udtdBifsPerAddr[address].createDate ||
      hubFile.createDate.getTime() < udtdBifsPerAddr[address].createDate.getTime()
    ) {
      udtdBifsPerAddr[address].createDate = hubFile.createDate;
    }
    if (
      !udtdBifsPerAddr[address].updateDate ||
      hubFile.updateDate.getTime() > udtdBifsPerAddr[address].updateDate.getTime()
    ) {
      udtdBifsPerAddr[address].updateDate = hubFile.updateDate;
    }
  }
  for (const backupFile of backupFiles) {
    const hubFile = hubFilesPerPath[backupFile.path];
    if (isObject(hubFile)) continue;

    let doUpdate = false, udtdFileInfo;
    const fileInfo = fileInfosPerPath[backupFile.path];
    if (isObject(fileInfo)) {
      if (fileInfo.status !== DELETED || backupFile.size !== fileInfo.size) {
        [doUpdate, udtdFileInfo] = [true, { ...fileInfo }];
      }
    } else {
      const [path, createDate] = [backupFile.path, backupFile.createDate];
      [doUpdate, udtdFileInfo] = [true, { path, createDate }];
    }
    if (doUpdate) {
      udtdFileInfo.status = DELETED;
      udtdFileInfo.size = backupFile.size;
      udtdFileInfo.updateDate = backupFile.updateDate;
      udtdFileInfos.push(udtdFileInfo);
    }
  }
  for (const [address, info] of Object.entries(udtdBifsPerAddr)) {
    let doUpdate = false, udtdBucketInfo;
    const bucketInfo = bucketInfosPerAddress[address];
    if (isObject(bucketInfo)) {
      if (info.nItems !== bucketInfo.nItems || info.size !== bucketInfo.size) {
        [doUpdate, udtdBucketInfo] = [true, { ...bucketInfo }];
      }
    } else {
      [doUpdate, udtdBucketInfo] = [true, { address, assoIssAddress: 'n/a' }];
    }
    if (doUpdate) {
      udtdBucketInfo.nItems = info.nItems;
      udtdBucketInfo.size = info.size;
      udtdBucketInfo.createDate = info.createDate;
      udtdBucketInfo.updateDate = info.updateDate;
      udtdBucketInfos.push(udtdBucketInfo);
    }
  }

  await updateFileInfos(udtdFileInfos);
  await updateBucketInfos(udtdBucketInfos);

  // Alert newer FileLogs
  let latestKeys = [], latestCreateDate, newerFileLogs = [];
  for (const log of latestFileLogs) {
    latestKeys.push(log.key);
    if (!latestCreateDate || latestCreateDate.getTime() < log.createDate.getTime()) {
      latestCreateDate = log.createDate;
    }
  }
  if (!latestCreateDate) latestCreateDate = startDate;
  const _newerFileLogs = await getNewerFileLogs(latestCreateDate);
  for (const log of _newerFileLogs) {
    if (latestKeys.includes(log.key)) continue;
    newerFileLogs.push(log);
  }
  if (newerFileLogs.length > 0) {
    console.log('*** Newer file logs ***');
    for (const log of newerFileLogs) {
      console.log(log);
    }
    console.log('*** End ***');
  }

  // Save latest processed keys and timestamp of FileLog
  await saveFileWorkLog(latestKeys, latestCreateDate);

  console.log(`(${logKey}) Rework finishes on ${(new Date()).toISOString()}.`);
};

rework();
