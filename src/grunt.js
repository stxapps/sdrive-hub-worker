import dataApi from './data';
import { BACKUP_BUCKET, HUB_BUCKET } from './const';

// Manually backup files and save fileLogs if errors in sdrive-hub-tasker.
//
// 1. Query logs in Logs explorer to check if there was errors
// resource.type="cloud_run_revision" SEARCH("`) Error`") timestamp >= "2024-06-01T00:00:00Z" timestamp <= "2024-07-31T23:59:59Z"
//
// 2. Check in Storage browser if exist in bucket sdrive-hub and backup
// Filter objects: path
//
// 3. If in sdrive-hub but not in backup, manually copy.
const copyFile = async () => {
  const path = '19pFThzqfdSBHroS5UdKW611fs96PCBcm1/tags/1721474642928-PNJq/i0000n_/1722244080492/1722244080492/1722082081265-ihQW-sdkf-1722082134188.json';
  await dataApi.copyFile(HUB_BUCKET, path, BACKUP_BUCKET);
};
copyFile();
//
// 4. If error saveFileLog, manually save.
const saveFileLog = async () => {
  // Manually add to FileLog and wait sdrive-hub-worker to use it
  //   to update FileInfo and BucketInfo?
  // Need to check time when log should happen V.S. time for worker to pick?

  // Or should directly update to FileInfo and BucketInfo?

};
//saveFileLog();

// Consistency between storage sdrive-hub, backup, FileInfo and BucketInfo per address
//
// 1. List files with prefix = address from both sdrive-hub and backup
// 2. Query FileInfo and BucketInfo with specified address
// 3. Check consistency
//    - All hubFiles should be in backupFiles. backupFiles can contain deleted files.
//    - All hubFiles should be in fileInfos. fileInfos can contain deleted files.
//    - All hubFiles should be calculated to bucketInfo except createDate.
//    - backupFiles and fileInfos should be the same.
//    - fileInfos should be calculated to bucketInfo except createDate.
//
// Tips: use Storage browser and filter with address for a glimpse.
const listFiles = async () => {
  const address = '18uDzPVE8wfEqgZ6KqZY4b6ZeP1GhkeNxG';
  const hubFiles = await dataApi.listFiles(HUB_BUCKET, address);
  for (const hubFile of hubFiles) {
    console.log(hubFile);
  }

  const backupFiles = await dataApi.listFiles(BACKUP_BUCKET, address);
  for (const backupFile of backupFiles) {
    console.log(backupFile);
  }

  const fileInfos = await dataApi.getFileInfosPerAddr(address);
  for (const fileInfo of fileInfos) {
    console.log(fileInfo);
  }

  const bucketInfos = await dataApi.getBucketInfos(address);
  for (const bucketInfo of bucketInfos) {
    console.log(bucketInfo);
  }
};
//listFiles();
