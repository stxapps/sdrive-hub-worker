import dataApi from './data';
import { HUB_BUCKET, BACKUP_BUCKET, ACTIVE, DELETED } from './const';
import { isObject, randomString, extractPath } from './utils';

const rework = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) rework starts on ${startDate.toISOString()}`);

  // Datastore
  const latestFileLogs = await dataApi.getLatestFileLogs();
  console.log(`(${logKey}) Got ${latestFileLogs.length} FileLog entities`);
  const fileInfos = await dataApi.getAllFileInfos();
  console.log(`(${logKey}) Got ${fileInfos.length} FileInfo entities`);
  const bucketInfos = await dataApi.getAllBucketInfos();
  console.log(`(${logKey}) Got ${bucketInfos.length} BucketInfo entities`);

  const fileInfosPerPath = {}, bucketInfosPerAddress = {};
  for (const fileInfo of fileInfos) {
    fileInfosPerPath[fileInfo.path] = fileInfo;
  }
  for (const bucketInfo of bucketInfos) {
    bucketInfosPerAddress[bucketInfo.address] = bucketInfo;
  }

  // Storage
  const hubFiles = await dataApi.listFiles(HUB_BUCKET);
  console.log(`(${logKey}) There are ${hubFiles.length} files in the hub bucket`);
  const backupFiles = await dataApi.listFiles(BACKUP_BUCKET);
  console.log(`(${logKey}) There are ${backupFiles.length} files in the backup bucket`);

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
      console.log(`(${logKey}) Copied ${hubFile.path} to the backup bucket`);
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
      if (
        info.nItems !== bucketInfo.nItems ||
        info.size !== bucketInfo.size ||
        info.updateDate.getTime() !== bucketInfo.updateDate.getTime()
      ) {
        [doUpdate, udtdBucketInfo] = [true, { ...bucketInfo }];
      }
    } else {
      const [assoIssAddress, createDate] = ['n/a', info.createDate];
      [doUpdate, udtdBucketInfo] = [true, { address, assoIssAddress, createDate }];
    }
    if (doUpdate) {
      udtdBucketInfo.nItems = info.nItems;
      udtdBucketInfo.size = info.size;
      udtdBucketInfo.updateDate = info.updateDate;
      udtdBucketInfos.push(udtdBucketInfo);
    }
  }
  console.log(`(${logKey}) Populated udtdFileInfos and udtdBucketInfos`);

  await dataApi.updateFileInfos(udtdFileInfos);
  console.log(`(${logKey}) Saved updated FileInfo entities`);
  await dataApi.updateBucketInfos(udtdBucketInfos);
  console.log(`(${logKey}) Saved updated BucketInfo entities`);

  // Unused fileInfos and bucketInfos
  const hbFilePaths = hubFiles.map(hubFile => hubFile.path);
  for (const backupFile of backupFiles) {
    if (hbFilePaths.includes(backupFile.path)) continue;
    hbFilePaths.push(backupFile.path);
  }
  for (const fileInfo of fileInfos) {
    if (hbFilePaths.includes(fileInfo.path)) continue;
    console.log(`(${logKey}) Unused fileInfo path`, fileInfo.path);
  }
  for (const bucketInfo of bucketInfos) {
    if (isObject(udtdBifsPerAddr[bucketInfo.address])) continue;
    console.log(`(${logKey}) Unused bucketInfo address`, bucketInfo.address);
  }

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
  console.log(`(${logKey}) Got ${newerFileLogs.length} newer FileLog entities`);
  if (newerFileLogs.length > 0) {
    console.log(`(${logKey}) newerFileLogs: ${JSON.stringify(newerFileLogs)}`);
  }

  // Save latest processed keys and timestamp of FileLog
  await dataApi.saveFileWorkLog(latestKeys, latestCreateDate);
  console.log(`(${logKey}) Saved latest FileWorkLog`);

  console.log(`(${logKey}) Rework finishes on ${(new Date()).toISOString()}.`);
};

rework();
