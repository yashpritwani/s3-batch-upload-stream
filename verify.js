require('dotenv').config();
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const util = require('util');

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
  httpOptions: {
    timeout: 166400000,
    connectTimeout: 120000
  }
});
const bucketName = process.env.S3_BUCKET_NAME;
const s3 = new AWS.S3();
const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);

const calculateMD5 = (filePath) => {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5');
    const stream = fs.createReadStream(filePath);

    stream.on('data', (data) => hash.update(data));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', (err) => reject(err));
  });
};

const calculateMultipartMD5 = (filePath, partSize) => {
  return new Promise((resolve, reject) => {
    const parts = [];
    const readStream = fs.createReadStream(filePath, { highWaterMark: partSize });
    
    readStream.on('data', (chunk) => {
      const hash = crypto.createHash('md5').update(chunk).digest('hex');
      parts.push(hash);
    });

    readStream.on('end', () => {
      const multipartHash = crypto.createHash('md5').update(Buffer.from(parts.join(''), 'hex')).digest('hex');
      resolve(multipartHash);
    });

    readStream.on('error', (err) => reject(err));
  });
};

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

const verifyFiles = async (localDir, bucketName) => {
  try {
    const s3Files = await listAllObjects(bucketName);
    const s3FilesMap = new Map(s3Files.map(file => [file.Key, file.ETag.replace(/"/g, '')]));

    const localFiles = await walkDirectory(localDir);
    console.log(`Found ${localFiles.length} local files to verify.`);

    const verificationResults = [];
    const failedVerifications = [];

    for (const file of localFiles) {
      const relativePath = path.relative(localDir, file);
      const s3Key = path.join('', relativePath).replace(/\\/g, '/');
      const fileSize = (await stat(file)).size;

      let localMD5;
      if (fileSize > 3 * 1024 * 1024) {
        localMD5 = await calculateMultipartMD5(file, 3 * 1024 * 1024);
      } else {
        localMD5 = await calculateMD5(file);
      }

      const s3ETag = s3FilesMap.get(s3Key);
      const result = {
        file: s3Key,
        localMD5,
        s3ETag,
        status: localMD5 === s3ETag ? 'Verified' : 'MD5 Mismatch'
      };

      verificationResults.push(result);

      if (localMD5 !== s3ETag) {
        failedVerifications.push(result);
      }
    }

    writeReport('verification_report.txt', verificationResults);
    writeReport('failed_verifications.txt', failedVerifications);
  } catch (error) {
    console.error('Error verifying files:', error);
  }
};

const writeReport = (outputFile, results) => {
  const stream = fs.createWriteStream(outputFile);
  let currentDirectory = null;

  results.forEach(result => {
    const directory = path.dirname(result.file);
    if (directory !== currentDirectory) {
      if (currentDirectory !== null) {
        stream.write('\n');
      }
      currentDirectory = directory;
      stream.write(`Directory: ${directory}\n`);
    }
    stream.write(`  ${result.file} - Status: ${result.status}\n`);
    stream.write(`    Local MD5: ${result.localMD5}\n`);
    stream.write(`    S3 ETag: ${result.s3ETag}\n`);
  });

  stream.end();
};

const walkDirectory = async (dir) => {
  let files = [];
  const items = await readdir(dir);
  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stats = await stat(fullPath);
    if (stats.isDirectory()) {
      files = files.concat(await walkDirectory(fullPath));
    } else if (stats.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
};

const main = async () => {
  const localDir = process.argv[2];
  if (!localDir) {
    console.log('Please provide the local directory to verify against S3.');
    process.exit(1);
  }
  await verifyFiles(localDir, bucketName);
};

main();
