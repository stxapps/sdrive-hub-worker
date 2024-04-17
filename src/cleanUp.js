import dataApi from './data';
import { BACKUP_BUCKET } from './const';
import { randomString } from './utils';

const doDryRun = process.argv[2] === '--dry-run';

const cleanUp = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) cleanUp starts on ${startDate.toISOString()}`);
  console.log(`(${logKey}) doDryRun: ${doDryRun}`);

  // Backup Storage and FileInfo
  const fileInfos = await dataApi.getDeletedFileInfos();
  console.log(`(${logKey}) Got ${fileInfos.length} FileInfo entities`);
  if (fileInfos.length > 0) {
    for (const fileInfo of fileInfos) {
      console.log(fileInfo);
    }

    const paths = fileInfos.map(fileInfo => fileInfo.path);
    if (!doDryRun) await dataApi.deleteFiles(BACKUP_BUCKET, paths);
    console.log(`(${logKey}) Deleted in the backup bucket`);

    if (!doDryRun) await dataApi.deleteFileInfos(fileInfos);
    console.log(`(${logKey}) Deleted the FileInfo entities`);
  }

  // FileLog
  const fileLogs = await dataApi.getObsoleteFileLogs();
  console.log(`(${logKey}) Got ${fileLogs.length} obsolete FileLog entities`);
  if (fileLogs.length > 0) {
    for (const fileLog of fileLogs) {
      console.log(fileLog);
    }

    if (!doDryRun) await dataApi.deleteFileLogs(fileLogs);
    console.log(`(${logKey}) Deleted the FileLog entities`);
  }

  // BucketInfo: Do manually on GCloud Console for now.

  // FileWorkLog: Do manually on GCloud Console for now.

  console.log(`(${logKey}) cleanUp finishes on ${(new Date()).toISOString()}.`);
};

cleanUp();
