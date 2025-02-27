import dataApi from './data'; // Mock test: import dataApi from './mock-data';
import { ACTIVE, DELETED, CREATE_FILE, UPDATE_FILE, DELETE_FILE } from './const';
import { isObject, isString, isNumber, randomString, extractPath } from './utils';

export const deriveAssoIssAddress = (assoIssAddress, newAssoIssAddress) => {
  if (!isString(assoIssAddress)) return newAssoIssAddress;
  if (!isString(newAssoIssAddress) || newAssoIssAddress === 'n/a') return assoIssAddress;
  return newAssoIssAddress;
};

const _main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker starts on ${startDate.toISOString()}`);

  // No need as no network connections
  /*process.on('SIGTERM', () => {
    console.log(`(${logKey}) received SIGTERM`);
    process.exit(0);
  });*/

  let lastKeys = [], lastCreateDate = new Date('1970-01-01T00:00:00.000Z');
  const fileWorkLog = await dataApi.getLatestFileWorkLog();
  console.log(`(${logKey}) Got ${fileWorkLog ? '1' : '0'} latest FileWorkLog entity`);
  if (isObject(fileWorkLog)) {
    [lastKeys, lastCreateDate] = [fileWorkLog.lastKeys, fileWorkLog.lastCreateDate];
  }
  console.log(`(${logKey}) Latest FileWorkLog lastCreateDate: ${lastCreateDate}`);

  const udtdFifsPerPath = {}, udtdBifsPerAddr = {};
  const fileLogs = await dataApi.getFileLogs(lastCreateDate);
  console.log(`(${logKey}) Got ${fileLogs.length} FileLog entities`);
  for (const fileLog of fileLogs) {
    if (lastKeys.includes(fileLog.key)) continue;

    const { address } = extractPath(fileLog.path);

    if (!isObject(udtdFifsPerPath[fileLog.path])) {
      udtdFifsPerPath[fileLog.path] = { address };
    }
    if ([CREATE_FILE, UPDATE_FILE].includes(fileLog.action)) {
      udtdFifsPerPath[fileLog.path].status = ACTIVE;
      udtdFifsPerPath[fileLog.path].size = fileLog.size;
    } else if ([DELETE_FILE].includes(fileLog.action)) {
      udtdFifsPerPath[fileLog.path].status = DELETED;
    } else {
      console.log(`(${logKey}) Invalid fileLog.action: ${JSON.stringify(fileLog)}`);
      continue;
    }
    if (
      !isObject(udtdFifsPerPath[fileLog.path].createDate) ||
      fileLog.createDate.getTime() < udtdFifsPerPath[fileLog.path].createDate.getTime()
    ) {
      udtdFifsPerPath[fileLog.path].createDate = fileLog.createDate;
    }
    if (
      !isObject(udtdFifsPerPath[fileLog.path].updateDate) ||
      fileLog.createDate.getTime() > udtdFifsPerPath[fileLog.path].updateDate.getTime()
    ) {
      udtdFifsPerPath[fileLog.path].updateDate = fileLog.createDate;
    }

    if (!isObject(udtdBifsPerAddr[address])) {
      udtdBifsPerAddr[address] = { nItems: 0, size: 0 };
    }
    udtdBifsPerAddr[address].assoIssAddress = deriveAssoIssAddress(
      udtdBifsPerAddr[address].assoIssAddress, fileLog.assoIssAddress
    );
    if ([CREATE_FILE].includes(fileLog.action)) {
      udtdBifsPerAddr[address].nItems += 1;
    } else if ([UPDATE_FILE].includes(fileLog.action)) {
      udtdBifsPerAddr[address].nItems += 0;
    } else if ([DELETE_FILE].includes(fileLog.action)) {
      udtdBifsPerAddr[address].nItems -= 1;
    } else {
      console.log(`(${logKey}) Invalid fileLog.action: ${JSON.stringify(fileLog)}`);
      continue;
    }
    udtdBifsPerAddr[address].size += fileLog.sizeChange;
    if (
      !isObject(udtdBifsPerAddr[address].createDate) ||
      fileLog.createDate.getTime() < udtdBifsPerAddr[address].createDate.getTime()
    ) {
      udtdBifsPerAddr[address].createDate = fileLog.createDate;
    }
    if (
      !isObject(udtdBifsPerAddr[address].updateDate) ||
      fileLog.createDate.getTime() > udtdBifsPerAddr[address].updateDate.getTime()
    ) {
      udtdBifsPerAddr[address].updateDate = fileLog.createDate;
    }
  }
  console.log(`(${logKey}) Populated udtdFifsPerPath and udtdBifsPerAddr`);

  const fileInfos = await dataApi.getFileInfos(Object.keys(udtdFifsPerPath));
  console.log(`(${logKey}) Got ${fileInfos.length} FileInfo entities`);
  const bucketInfos = await dataApi.getBucketInfos(Object.keys(udtdBifsPerAddr));
  console.log(`(${logKey}) Got ${bucketInfos.length} BucketInfo entities`);

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
        (isNumber(info.size) && info.size !== fileInfo.size) ||
        info.updateDate.getTime() !== fileInfo.updateDate.getTime()
      ) {
        [doUpdate, udtdFileInfo] = [true, { ...fileInfo }];
      }
    } else {
      const [address, createDate] = [info.address, info.createDate];
      [doUpdate, udtdFileInfo] = [true, { path, address, size: 0, createDate }];
    }
    if (doUpdate) {
      udtdFileInfo.status = info.status;
      if (isNumber(info.size)) udtdFileInfo.size = info.size;
      udtdFileInfo.updateDate = info.updateDate;
      udtdFileInfos.push(udtdFileInfo);
    }
  }
  for (const [address, info] of Object.entries(udtdBifsPerAddr)) {
    let udtdBucketInfo;
    const bucketInfo = bucketInfosPerAddress[address];
    if (isObject(bucketInfo)) {
      udtdBucketInfo = { ...bucketInfo };
    } else {
      const createDate = info.createDate;
      udtdBucketInfo = { address, nItems: 0, size: 0, createDate };
    }
    udtdBucketInfo.assoIssAddress = deriveAssoIssAddress(
      udtdBucketInfo.assoIssAddress, info.assoIssAddress
    );
    udtdBucketInfo.nItems += info.nItems;
    udtdBucketInfo.size += info.size;
    udtdBucketInfo.updateDate = info.updateDate;
    udtdBucketInfos.push(udtdBucketInfo);
  }
  console.log(`(${logKey}) Populated udtdFileInfos and udtdBucketInfos`);

  // updateFileInfos, updateBucketInfos, and saveFileWorkLog in one transaction
  //   for consistency and can retry, but might bad performance!

  await dataApi.updateFileInfos(udtdFileInfos);
  console.log(`(${logKey}) Saved updated FileInfo entities`);
  await dataApi.updateBucketInfos(udtdBucketInfos);
  console.log(`(${logKey}) Saved updated BucketInfo entities`);

  let latestKeys = [], latestCreateDate = startDate;
  for (let i = fileLogs.length - 1; i >= 0; i--) {
    const { key, createDate } = fileLogs[i];

    if (i === fileLogs.length - 1) latestCreateDate = createDate;
    if (latestKeys.length >= 10 && latestCreateDate.getTime() > createDate.getTime()) {
      break;
    }
    latestKeys.push(key);
  }
  await dataApi.saveFileWorkLog(latestKeys, latestCreateDate);
  console.log(`(${logKey}) Saved latest FileWorkLog`);

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
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
