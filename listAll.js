require('dotenv').config();
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
  httpOptions: {
    timeout: 166400000,
    connectTimeout: 120000
  }
});

const s3 = new AWS.S3();
const bucketName = process.env.S3_BUCKET_NAME;

const listAllObjects = async (bucket, prefix = '') => {
  let isTruncated = true;
  let marker;
  const fileList = [];

  while (isTruncated) {
    let params = { Bucket: bucket };
    if (prefix) params.Prefix = prefix;
    if (marker) params.Marker = marker;

    try {
      const response = await s3.listObjects(params).promise();
      response.Contents.forEach(item => {
        fileList.push(item);
      });
      isTruncated = response.IsTruncated;
      if (isTruncated) {
        marker = response.Contents[response.Contents.length - 1].Key;
      }
    } catch (error) {
      console.error('Error listing objects:', error);
      isTruncated = false;
    }
  }

  return fileList;
};

const formatFileList = (fileList) => {
  const directoryMap = new Map();

  fileList.forEach(file => {
    const dirname = path.dirname(file.Key);
    if (!directoryMap.has(dirname)) {
      directoryMap.set(dirname, []);
    }
    directoryMap.get(dirname).push({ Key: file.Key, Size: file.Size });
  });

  return directoryMap;
};

const writeToFile = (directoryMap, fileList, outputFile) => {
  const stream = fs.createWriteStream(outputFile);
  let totalSize = 0;
  let totalFiles = fileList.length;
  const folderSummaries = [];

  directoryMap.forEach((files, directory) => {
    let folderSize = 0;
    files.forEach(file => {
      folderSize += file.Size;
    });
    folderSummaries.push({ directory, totalFiles: files.length, totalSize: folderSize });

    totalSize += folderSize;
  });

  const totalSizeGB = (totalSize / (1024 * 1024 * 1024)).toFixed(2);
  
  // Write total summary at the top
  stream.write(`Total Files: ${totalFiles}\n`);
  stream.write(`Total Size: ${totalSizeGB} GB\n\n`);

  folderSummaries.forEach(summary => {
    const folderSizeMB = (summary.totalSize / (1024 * 1024)).toFixed(2);
    stream.write(`Directory: ${summary.directory}\n`);
    stream.write(`  Total Files: ${summary.totalFiles}\n`);
    stream.write(`  Total Size: ${folderSizeMB} MB\n\n`);
  });

  directoryMap.forEach((files, directory) => {
    stream.write(`Directory: ${directory}\n`);
    files.forEach(file => {
      const fileSizeMB = (file.Size / (1024)).toFixed(2);
      stream.write(`  ${file.Key} - ${fileSizeMB} KB\n`);
    });
    const folderSummary = folderSummaries.find(summary => summary.directory === directory);
    const folderSizeMB = (folderSummary.totalSize / (1024 * 1024)).toFixed(2);
    stream.write(`Total Files: ${folderSummary.totalFiles}\n`);
    stream.write(`Total Size: ${folderSizeMB} MB\n`);
    stream.write('\n');
  });

  stream.end();
};

const main = async () => {
  try {
    const fileList = await listAllObjects(bucketName);
    const directoryMap = formatFileList(fileList);
    writeToFile(directoryMap, fileList, 's3_file_list.txt');
    console.log('File list written to s3_file_list.txt');
  } catch (error) {
    console.error('Error:', error);
  }
};

main();
