import { Datastore, PropertyFilter } from '@google-cloud/datastore';

import {
  FILE_LOG, FILE_INFO, BUCKET_INFO, FILE_WORK_LOG, ACTIVE, DELETED, PUT_FILE,
  DELETE_FILE, MOVE_FILE_PUT_STEP, MOVE_FILE_DEL_STEP,
} from './const';
import { isObject, isString, randomString, extractPath } from './utils';

const datastore = new Datastore();

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
        key: entity[datastore.KEY].id,
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

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker starts on ${startDate.toISOString()}`);

  let lastKeys = [], lastCreateDate = new Date('1970-01-01T00:00:00.000Z');
  const fileWorkLog = await getLatestFileWorkLog();
  if (isObject(fileWorkLog)) {
    lastKeys = fileWorkLog.lastKeys;
    lastCreateDate = fileWorkLog.lastCreateDate;
  }

  const udtdFifsPerPath = {}, udtdBifsPerAddr = {};
  const fileLogs = await getFileLogs(lastCreateDate);
  for (const fileLog of fileLogs) {
    if (lastKeys.includes(fileLog.key)) continue;

    if (!isObject(udtdFifsPerPath[fileLog.path])) {
      udtdFifsPerPath[fileLog.path] = {
        size: 0, createDate: null, updateDate: null,
      };
    }
    if ([PUT_FILE, MOVE_FILE_PUT_STEP].includes(fileLog.action)) {
      udtdFifsPerPath[fileLog.path].status = ACTIVE;
      udtdFifsPerPath[fileLog.path].size = fileLog.size;
    } else if ([DELETE_FILE, MOVE_FILE_DEL_STEP].includes(fileLog.action)) {
      udtdFifsPerPath[fileLog.path].status = DELETED;
    } else {
      console.log('Invalid fileLog.action:', fileLog);
      continue;
    }
    if (
      !udtdFifsPerPath[fileLog.path].createDate ||
      fileLog.createDate.getTime() < udtdFifsPerPath[fileLog.path].createDate.getTime()
    ) {
      udtdFifsPerPath[fileLog.path].createDate = fileLog.createDate;
    }
    if (
      !udtdFifsPerPath[fileLog.path].updateDate ||
      fileLog.createDate.getTime() > udtdFifsPerPath[fileLog.path].updateDate.getTime()
    ) {
      udtdFifsPerPath[fileLog.path].updateDate = fileLog.createDate;
    }

    const { address } = extractPath(fileLog.path);
    if (!isObject(udtdBifsPerAddr[address])) {
      udtdBifsPerAddr[address] = {
        nItems: 0, size: 0, createDate: null, updateDate: null,
      };
    }
    udtdBifsPerAddr[address].assoIssAddress = fileLog.assoIssAddress;
    if ([PUT_FILE, MOVE_FILE_PUT_STEP].includes(fileLog.action)) {
      udtdBifsPerAddr[address].nItems += 1;
    } else if ([DELETE_FILE, MOVE_FILE_DEL_STEP].includes(fileLog.action)) {
      udtdBifsPerAddr[address].nItems -= 1;
    } else {
      console.log('Invalid fileLog.action:', fileLog);
      continue;
    }
    udtdBifsPerAddr[address].size += fileLog.sizeChange;
    if (
      !udtdBifsPerAddr[address].createDate ||
      fileLog.createDate.getTime() < udtdBifsPerAddr[address].createDate.getTime()
    ) {
      udtdBifsPerAddr[address].createDate = fileLog.createDate;
    }
    if (
      !udtdBifsPerAddr[address].updateDate ||
      fileLog.createDate.getTime() > udtdBifsPerAddr[address].updateDate.getTime()
    ) {
      udtdBifsPerAddr[address].updateDate = fileLog.createDate;
    }
  }

  const fileInfos = await getFileInfos(Object.keys(udtdFifsPerPath));
  const bucketInfos = await getBucketInfos(Object.keys(udtdBifsPerAddr));

  const fileInfosPerPath = {}, bucketInfosPerAddress = {};
  for (const fileInfo of fileInfos) {
    fileInfosPerPath[fileInfo.path] = fileInfo;
  }
  for (const bucketInfo of bucketInfos) {
    bucketInfosPerAddress[bucketInfo.address] = bucketInfo;
  }

  const udtdFileInfos = [], udtdBucketInfos = [];
  for (const [path, info] of Object.entries(udtdFifsPerPath)) {
    let doUpdate = false, udtdFileInfo;
    const fileInfo = fileInfosPerPath[path];
    if (isObject(fileInfo)) {
      if (
        info.status !== fileInfo.status ||
        info.size !== fileInfo.size ||
        info.updateDate.getTime() !== fileInfo.updateDate.getTime()
      ) {
        [doUpdate, udtdFileInfo] = [true, { ...fileInfo }];
      }
    } else {
      const createDate = info.createDate;
      [doUpdate, udtdFileInfo] = [true, { path, createDate }];
    }
    if (doUpdate) {
      udtdFileInfo.status = info.status;
      udtdFileInfo.size = info.size;
      udtdFileInfo.updateDate = info.updateDate;
      udtdFileInfos.push(udtdFileInfo);
    }
  }
  for (const [address, info] of Object.entries(udtdBifsPerAddr)) {
    let doUpdate = false, udtdBucketInfo;
    const bucketInfo = bucketInfosPerAddress[address];
    if (isObject(bucketInfo)) {
      if (
        info.nItems !== bucketInfo.nItems ||
        info.size !== bucketInfo.size ||
        info.updateDate.getTime() !== bucketInfo.updateDate.getTime()
      ) {
        [doUpdate, udtdBucketInfo] = [true, { ...bucketInfo }];
      }
    } else {
      [doUpdate, udtdBucketInfo] = [true, { address }];
    }
    if (doUpdate) {
      udtdBucketInfo.assoIssAddress = info.assoIssAddress;
      udtdBucketInfo.nItems = info.nItems;
      udtdBucketInfo.size = info.size;
      udtdBucketInfo.createDate = info.createDate;
      udtdBucketInfo.updateDate = info.updateDate;
      udtdBucketInfos.push(udtdBucketInfo);
    }
  }

  await updateFileInfos(udtdFileInfos);
  await updateBucketInfos(udtdBucketInfos);

  let latestKeys = [], latestCreateDate = startDate;
  for (let i = fileLogs.length - 1; i >= 0; i--) {
    const { key, createDate } = fileLogs[i];

    if (i === fileLogs.length - 1) latestCreateDate = createDate;
    if (lastKeys.length >= 10 && latestCreateDate.getTime() > createDate.getTime()) {
      break;
    }
    latestKeys.push(key);
  }
  await saveFileWorkLog(latestKeys, latestCreateDate);
};

const main = async () => {
  try {
    await _main();
    process.exit(0);
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
};

main();
