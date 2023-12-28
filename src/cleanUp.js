import dataApi from './data';
import { BACKUP_BUCKET } from './const';
import { randomString } from './utils';

const cleanUp = async () => {
  const startDate = new Date();
  const logKey = randomString(12);
  console.log(`(${logKey}) cleanUp starts on ${startDate.toISOString()}`);

  // Backup Storage and FileInfo
  const fileInfos = await dataApi.getDeletedFileInfos();
  const paths = fileInfos.map(fileInfo => fileInfo.path);
  await dataApi.deleteFiles(BACKUP_BUCKET, paths);
  await dataApi.deleteFileInfos(fileInfos);

  // FileLog
  const fileLogs = await dataApi.getObsoleteFileLogs();
  await dataApi.deleteFileLogs(fileLogs);

  // BucketInfo: Do manually on GCloud Console for now.

  // FileWorkLog: Do manually on GCloud Console for now.

  console.log(`(${logKey}) CleanUp finishes on ${(new Date()).toISOString()}.`);
};

cleanUp();
