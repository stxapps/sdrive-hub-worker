import dataApi from './data';
import { HUB_BUCKET, BACKUP_BUCKET, ACTIVE, DELETED } from './const';
import { isObject, randomString, extractPath } from './utils';

const rework = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) rework starts on ${startDate.toISOString()}`);

  // Datastore
  const latestFileLogs = await dataApi.getLatestFileLogs();
  const fileInfos = await dataApi.getAllFileInfos();
  const bucketInfos = await dataApi.getAllBucketInfos();

  const fileInfosPerPath = {}, bucketInfosPerAddress = {};
  for (const fileInfo of fileInfos) {
    fileInfosPerPath[fileInfo.path] = fileInfo;
  }
  for (const bucketInfo of bucketInfos) {
    bucketInfosPerAddress[bucketInfo.address] = bucketInfo;
  }

  // Storage
  const hubFiles = await dataApi.listFiles(HUB_BUCKET);
  const backupFiles = await dataApi.listFiles(BACKUP_BUCKET);

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
      await dataApi.copyFile(HUB_BUCKET, hubFile.path, BACKUP_BUCKET);
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

  await dataApi.updateFileInfos(udtdFileInfos);
  await dataApi.updateBucketInfos(udtdBucketInfos);

  // Alert newer FileLogs
  let latestKeys = [], latestCreateDate, newerFileLogs = [];
  for (const log of latestFileLogs) {
    latestKeys.push(log.key);
    if (!latestCreateDate || latestCreateDate.getTime() < log.createDate.getTime()) {
      latestCreateDate = log.createDate;
    }
  }
  if (!latestCreateDate) latestCreateDate = startDate;
  const _newerFileLogs = await dataApi.getFileLogs(latestCreateDate);
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
  await dataApi.saveFileWorkLog(latestKeys, latestCreateDate);

  console.log(`(${logKey}) Rework finishes on ${(new Date()).toISOString()}.`);
};

rework();
