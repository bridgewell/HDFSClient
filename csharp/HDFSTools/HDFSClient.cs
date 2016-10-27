using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.IO;
using Newtonsoft.Json;


namespace HDFSTools
{

    public class _FileStatus
    {
        public long accessTime;
        public long blockSize;
        public int childrenNum;
        public int fileId;
        public string group;
        public long length;
        public long modificationTime;
        public string owner;
        public string pathSuffix;
        public string permission;
        public int replication;
        public string type;
    }

    public class ListStatusResponse
    {
        public class _FileStatuses
        {
            public IList<_FileStatus> FileStatus;
        }
        public _FileStatuses FileStatuses;
    }

    public class FileStatusResponse
    {
        public _FileStatus FileStatus;
    }

    public class NameNode
    {
        private string domain;
        private int port;

        public NameNode(string domain, int port)
        {
            this.domain = domain;
            this.port = port;
        }

        public override string ToString()
        {
            return String.Format("{0}:{1}", this.domain, this.port);
        }
    }

    public class HDFSClient
    {
        public static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private static readonly int TimeOutInSecond = 10;

        private DateTime lastTimeRefreshActiveNameNode;


        private class QueryUrl
        {
            public static readonly string NameNodeStatus = "http://{0}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus";
            public static readonly string WebHdfs = "http://{{0}}/webhdfs/v1/{0}";
            public static readonly string MakeDirs = "?op=MKDIRS";
            public static readonly string Delete = "?op=DELETE&recursive={0}";
            public static readonly string Rename = "?op=RENAME&destination={0}";
            public static readonly string Create = "?op=CREATE&overwrite={0}";
            public static readonly string ListDir = "?op=LISTSTATUS";
            public static readonly string Open = "?op=OPEN";
            public static readonly string Status = "?op=GETFILESTATUS";
        }

        private static class FileType
        {
            public const string File = "FILE";
            public const string Directory = "DIRECTORY";
        }

        public class ListResult
        {
            public string[] dirs { get; set; }
            public string[] files { get; set; }

            public bool isEmtpy()
            {
                return dirs.Count() + files.Count() == 0;
            }
        }

        private NameNode[] nns;
        private NameNode activeNameNode;
        private WebClient webClient;

        private string CombineUrl(params string[] urls)
        {
            return string.Join("/", (from url in urls
                                     where !string.IsNullOrWhiteSpace(url)
                                     select url.Trim('/')));
        }

        /// <summary>
        /// </summary>
        /// <param name="nns">list of NameNode address</param>
        public HDFSClient(NameNode[] nns)
        {
            InitHDFSClient(nns);
        }

        /// <summary>
        /// </summary>
        /// <param name="nns">list of NameNode addres</param>
        private void InitHDFSClient(NameNode[] nns)
        {
            this.nns = nns;
            this.webClient = new WebClient();
            RefreshActiveNameNode();
        }

        /// <summary>
        /// Looking for the active NameNode
        /// </summary>
        public void RefreshActiveNameNode()
        {
            int index;
            for (index = 0; index < this.nns.Length; ++index)
            {
                var nameNode = this.nns[index];
                HttpWebRequest request = (HttpWebRequest)WebRequest.CreateHttp(String.Format(QueryUrl.NameNodeStatus, nameNode));
                try
                {
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    StreamReader sr = new StreamReader(response.GetResponseStream());
                    string message = sr.ReadToEnd();
                    if (message.Contains("active"))
                    {
                        break;
                    }
                    response.Close();
                }
                catch (WebException)
                {
                    // Do nothing
                }
            }
            if (index == this.nns.Length)
            {
                throw new Exception("Can't find active HDFS NameNode");
            }

            this.activeNameNode = this.nns[index];
            if (index != 0)
            {
                this.nns[index] = this.nns[0];
                this.nns[0] = this.activeNameNode;
            }
            this.lastTimeRefreshActiveNameNode = DateTime.UtcNow;
        }

        private bool TrySendRequest(string url, string method, int errBoundary = 2)
        {

            int errCount = 0;
            while (errCount < errBoundary)
            {
                try
                {
                    var isSuccess = SendRequest(url, method);
                    if (isSuccess)
                    {
                        return true;
                    }
                    else
                    {
                        throw new WebException();
                    }
                }
                catch (WebException ex)
                {
                    errCount++;
                    if (errCount < errBoundary)
                    {
                        logger.Warn(String.Format("Can't send request: {0} with message {1}, try to refresh active HDFS NameNode",
                                                  String.Format(url, this.activeNameNode),
                                                  ex.Message));
                        RefreshActiveNameNode();
                    }
                    else
                    {
                        logger.Error(String.Format("Can't send request: {0} with error message {1}",
                                                   String.Format(url, this.activeNameNode),
                                                   ex.Message));
                    }
                }
            }
            return false;
        }

        private bool SendRequest(string url, string method)
        {
            TimeSpan ts = DateTime.UtcNow - this.lastTimeRefreshActiveNameNode;
            if (ts.TotalSeconds >= TimeOutInSecond)
            {
                RefreshActiveNameNode();
            }
            url = string.Format(url, activeNameNode);
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.Method = method;
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            var isSuccess = response.StatusCode == HttpStatusCode.OK;
            response.Close();
            return isSuccess;
        }

        public bool Makedirs(string path)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, path.TrimStart('/')), QueryUrl.MakeDirs);
            return TrySendRequest(url, "PUT");
        }

        public bool Rename(string src, string dst)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, src.TrimStart('/')),
                                    String.Format(QueryUrl.Rename, dst));
            return TrySendRequest(url, "PUT");
        }

        private bool Delete(string path, bool recursive = false)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, path.TrimStart('/')),
                                    String.Format(QueryUrl.Delete, recursive));
            return TrySendRequest(url, "DELETE");
        }

        private string GenerateTmpPath(string path)
        {
            string guid = Guid.NewGuid().ToString();
            return path + "_" + guid + "_tmp";
        }

        private bool UploadFile(string localPath, string remotePath, int errBoundary = 3)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, remotePath.TrimStart('/')),
                                    String.Format(QueryUrl.Create, true));

            int errCount = 0;
            while (errCount < errBoundary)
            {
                try
                {
                    this.webClient.UploadFile(string.Format(url, this.activeNameNode), "PUT", localPath);
                    break;
                }
                catch (WebException we)
                {
                    RefreshActiveNameNode();
                    errCount++;
                }
            }
            return errCount < errBoundary ? true : false;
        }

        private bool UploadByTmpFile(string localPath, string remotePath)
        {
            string remoteTmpPath = GenerateTmpPath(remotePath);
            bool success = UploadFile(localPath, remoteTmpPath);
            if (success)
            {
                bool renameSuccess = Rename(remoteTmpPath, remotePath);
                if (!renameSuccess || ListDir(remoteTmpPath) != null)
                {
                    Delete(remoteTmpPath);
                    return false;
                }
                return true;
            }
            else
            {
                Delete(remoteTmpPath);
                return false;
            }
        }

        private bool UploadDirectory(string localPath, string remotePath)
        {

            Queue<string> dirQueue = new Queue<string>();

            dirQueue.Enqueue("");
            while (dirQueue.Count != 0)
            {
                string pathSuffix = dirQueue.Dequeue();
                string rPath = CombineUrl(remotePath, pathSuffix);
                string lPath = Path.Combine(localPath, pathSuffix);

                Makedirs(rPath);

                foreach (var filePath in Directory.GetFiles(lPath))
                {
                    var fileName = Path.GetFileName(filePath);
                    var w = CombineUrl(rPath, fileName);
                    UploadFile(filePath,
                               CombineUrl(rPath, fileName));
                }
                foreach (var dirPath in Directory.GetDirectories(lPath))
                {
                    dirQueue.Enqueue(CombineUrl(pathSuffix, Path.GetFileName(dirPath)));
                }
            }

            return true;
        }

        private bool UploadByTmpDirectory(string localPath, string remotePath)
        {
            string remoteTmpPath = GenerateTmpPath(remotePath);
            bool success = UploadDirectory(localPath, remoteTmpPath);
            if (success)
            {
                bool renameSuccess = Rename(remoteTmpPath, remotePath);
                if (!renameSuccess || ListDir(remoteTmpPath) != null)
                {
                    Delete(remoteTmpPath);
                    return false;
                }
                return true;
            }
            else
            {
                Delete(remoteTmpPath);
                return false;
            }
        }

        public bool Upload(string localPath, string remotePath)
        {
            if (File.Exists(localPath))
            {
                return UploadByTmpFile(localPath, remotePath);
            }
            else if (Directory.Exists(localPath))
            {
                return UploadByTmpDirectory(localPath, remotePath);
            }
            else
            {
                logger.Warn(string.Format("Local path not exists: {0}", localPath));
                return false;
            }
        }

        /// <summary>
        /// List HDFS directory
        /// </summary>
        /// <param name="path"></param>
        /// <param name="errBoundary"></param>
        /// <returns>ListResult, null if path is not exists</returns>
        private ListResult ListDir(string path, int errBoundary = 2)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, path.TrimStart('/')), QueryUrl.ListDir);

            int errCount = 0;
            while (errCount < errBoundary)
            {
                try
                {
                    var request = (HttpWebRequest)WebRequest.Create(String.Format(url, this.activeNameNode));
                    request.Method = "GET";
                    var response = (HttpWebResponse)request.GetResponse();
                    StreamReader sr = new StreamReader(response.GetResponseStream());
                    var statuses = JsonConvert.DeserializeObject<ListStatusResponse>(sr.ReadToEnd());
                    response.Close();

                    var listResult = new ListResult();

                    listResult.dirs = (from status in statuses.FileStatuses.FileStatus
                                       where status.type == FileType.Directory && !string.IsNullOrEmpty(status.pathSuffix)
                                       select status.pathSuffix).ToArray();

                    listResult.files = (from status in statuses.FileStatuses.FileStatus
                                        where status.type == FileType.File && !string.IsNullOrEmpty(status.pathSuffix)
                                        select status.pathSuffix).ToArray();

                    return listResult;
                }
                catch (WebException)
                {
                    RefreshActiveNameNode();
                    errCount++;
                }
            }
            logger.Warn("Can't list HDFS directory");
            return null;
        }

        private _FileStatus GetStatus(string path, int errBoundary = 2)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, path.TrimStart('/')), QueryUrl.Status);

            int errCount = 0;
            while (errCount < errBoundary)
            {
                try
                {
                    var request = (HttpWebRequest)WebRequest.Create(String.Format(url, this.activeNameNode));
                    request.Method = "GET";
                    var response = (HttpWebResponse)request.GetResponse();
                    StreamReader sr = new StreamReader(response.GetResponseStream());
                    var statuses = JsonConvert.DeserializeObject<FileStatusResponse>(sr.ReadToEnd());
                    response.Close();
                    return statuses.FileStatus;
                }
                catch (WebException)
                {
                    RefreshActiveNameNode();
                    errCount++;
                }
            }
            logger.Warn("Can't query HDFS file status");
            return null;
        }

        private bool DownloadFile(string remotePath, string localPath, int errBoundary = 2)
        {
            string url = CombineUrl(String.Format(QueryUrl.WebHdfs, remotePath.TrimStart('/')), QueryUrl.Open);

            int errCount = 0;
            while (errCount < errBoundary)
            {
                try
                {
                    this.webClient.DownloadFile(string.Format(url, this.activeNameNode), localPath);
                    break;
                }
                catch (WebException)
                {
                    RefreshActiveNameNode();
                    errCount++;
                }
            }
            return errCount < errBoundary ? true : false;
        }

        private bool DownloadDirectory(string remotePath, string localPath, int errBoundary = 3)
        {
            Queue<string> dirQueue = new Queue<string>();

            dirQueue.Enqueue("");
            while (dirQueue.Count != 0)
            {
                string pathSuffix = dirQueue.Dequeue();
                string rPath = CombineUrl(remotePath, pathSuffix);
                string lPath = CombineUrl(localPath, pathSuffix);

                Directory.CreateDirectory(lPath);

                ListResult listResult = ListDir(rPath);

                foreach (var fileName in listResult.files)
                {
                    DownloadFile(CombineUrl(remotePath, pathSuffix, fileName),
                                 CombineUrl(localPath, pathSuffix, fileName));
                }

                foreach (var dirName in listResult.dirs)
                {
                    dirQueue.Enqueue(CombineUrl(pathSuffix, dirName));
                }
            }
            return true;
        }


        private bool DownloadByTmpFile(string remotePath, string localPath)
        {
            string localTmpPath = GenerateTmpPath(localPath);
            bool success = DownloadFile(remotePath, localTmpPath);
            if (success)
            {
                File.Move(localTmpPath, localPath);
            }
            else
            {
                File.Delete(localTmpPath);
            }
            return success;
        }

        private bool DownloadByTmpDirectory(string remotePath, string localPath)
        {
            string localTmpPath = GenerateTmpPath(localPath);
            bool success = DownloadDirectory(remotePath, localTmpPath);
            if (success)
            {
                Directory.Move(localTmpPath, localPath);
            }
            else
            {
                Directory.Delete(localTmpPath);
            }
            return success;
        }

        private void CheckAndRemoveTargetData(string path)
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
            else if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }

        public bool Download(string remotePath, string localPath, bool overwrite = true)
        {

            if (File.Exists(localPath) || Directory.Exists(localPath))
            {
                logger.Warn(string.Format("Local path have been used: {0}", localPath));
                if (!overwrite)
                {
                    return false;
                }
            }

            _FileStatus fileStatus = GetStatus(remotePath);
            if (fileStatus == null)
            {
                logger.Warn("Remote path is not exists, please check");
                return false;
            }

            switch (fileStatus.type)
            {
                case FileType.File:
                    return DownloadByTmpFile(remotePath, localPath);
                case FileType.Directory:
                    return DownloadByTmpDirectory(remotePath, localPath);
                default:
                    logger.Error(string.Format("Unknow file type: {0}", fileStatus.type));
                    return false;
            }
        }
    }
}