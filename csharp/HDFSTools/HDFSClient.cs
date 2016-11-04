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
        private readonly string domain;
        private readonly int port;

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
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private const int TimeOutInSecond = 10;
        private DateTime lastTimeRefreshActiveNameNode;
        private readonly WebAction webAction = new WebAction();


        private static class QueryUrl
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
            public string[] Dirs { get; set; }
            public string[] Files { get; set; }

            public bool IsEmtpy()
            {
                return Dirs.Count() + Files.Count() == 0;
            }
        }

        private NameNode[] nns;
        private NameNode activeNameNode;

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
            this.webAction.SetDefaultExceptionTrigger(ExceptionTrigger);
            RefreshActiveNameNode();
        }

        /// <summary>
        /// Looking for the active NameNode
        /// </summary>
        public void ExceptionTrigger(string url, WebException ex, bool isFinal)
        {
            if (!isFinal)
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

        private bool TrySendHdfsRequest(string url, string method)
        {
            Func<string, string, bool> action = (string _url, string _method) =>
            {
                var response = SendHdfsRequest(_url, _method);
                response.Close();
                return true;
            };
            return webAction.Retry(url, method, action, ExceptionTrigger);
        }

        private HttpWebResponse SendHdfsRequest(string url, string method)
        {
            string requestUrl = FormatNameNode(url);

            return SendRequest(requestUrl, method);
        }

        private HttpWebResponse SendRequest(string url, string method)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.Method = method;

            HttpWebResponse response = (HttpWebResponse)request.GetResponse();

            if (response.StatusCode != HttpStatusCode.OK)
            {
                string msg = String.Format("Receive status code: {0}", response.StatusCode);
                response.Close();
                throw new WebException(msg);
            }
            return response;
        }
        private string FormatNameNode(string url)
        {
            CheckForRefreshActiveNameNode();
            return String.Format(url, activeNameNode);
        }

        private void CheckForRefreshActiveNameNode()
        {
            TimeSpan ts = DateTime.UtcNow - this.lastTimeRefreshActiveNameNode;
            if (ts.TotalSeconds >= TimeOutInSecond)
            {
                RefreshActiveNameNode();
            }
        }

        public void RefreshActiveNameNode()
        {
            int index;
            for (index = 0; index < this.nns.Length; ++index)
            {
                var nameNode = this.nns[index];
                string url = String.Format(QueryUrl.NameNodeStatus, nameNode);
                Func<string, string, string> action = (string _url, string _method) =>
                {
                    var response = SendRequest(_url, _method);
                    StreamReader sr = new StreamReader(response.GetResponseStream());
                    string message = sr.ReadToEnd();
                    sr.Close();
                    response.Close();
                    return message;
                };

                string responseMessage = webAction.Retry(url, WebRequestMethods.Http.Get, action);
                if (responseMessage != null && responseMessage.Contains("active"))
                {
                    break;
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

        public bool Makedirs(string path)
        {
            string url = String.Format(QueryUrl.WebHdfs, path.TrimStart('/')) + QueryUrl.MakeDirs;
            return TrySendHdfsRequest(url, WebRequestMethods.Http.Put);
        }

        public bool Rename(string src, string dst)
        {
            string url = String.Format(QueryUrl.WebHdfs, src.TrimStart('/')) + String.Format(QueryUrl.Rename, dst);
            return TrySendHdfsRequest(url, WebRequestMethods.Http.Put);
        }

        private bool Delete(string path, bool recursive = false)
        {
            string url = String.Format(QueryUrl.WebHdfs, path.TrimStart('/')) + String.Format(QueryUrl.Delete, recursive);
            return TrySendHdfsRequest(url, "DELETE");
        }

        private string GenerateTmpPath(string path)
        {
            string guid = Guid.NewGuid().ToString();
            return path + "_" + guid + "_tmp";
        }

        private bool UploadFile(string localPath, string remotePath)
        {
            string url = String.Format(QueryUrl.WebHdfs, remotePath.Trim('/')) + String.Format(QueryUrl.Create, true);

            Func<string, string, bool> action = (_url, _method) =>
            {
                _url = FormatNameNode(_url);
                using (WebClient wc = new WebClient())
                {
                    wc.UploadFile(FormatNameNode(_url), _method, localPath);
                }
                return true;
            };

            return webAction.Retry(url, WebRequestMethods.Http.Put, action, ExceptionTrigger);
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
                    UploadFile(filePath, CombineUrl(rPath, fileName));
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
                if (GetStatus(remotePath) != null)
                {
                    Delete(remotePath, true);
                }

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
        private ListResult ListDir(string path)
        {
            string url = String.Format(QueryUrl.WebHdfs, path.TrimStart('/')) + QueryUrl.ListDir;


            Func<string, string, ListStatusResponse> action = (string _url, string _method) =>
            {
                var response = SendHdfsRequest(_url, _method);
                StreamReader sr = new StreamReader(response.GetResponseStream());
                ListStatusResponse listStatus = JsonConvert.DeserializeObject<ListStatusResponse>(sr.ReadToEnd());
                sr.Close();
                response.Close();
                return listStatus;
            };
            var statuses = webAction.Retry(url, WebRequestMethods.Http.Get, action, ExceptionTrigger);

            if (statuses == null)
            {
                return null;
            }

            var listResult = new ListResult();

            listResult.Dirs = (from status in statuses.FileStatuses.FileStatus
                               where status.type == FileType.Directory && !string.IsNullOrEmpty(status.pathSuffix)
                               select status.pathSuffix).ToArray();

            listResult.Files = (from status in statuses.FileStatuses.FileStatus
                                where status.type == FileType.File && !string.IsNullOrEmpty(status.pathSuffix)
                                select status.pathSuffix).ToArray();
            return listResult;
        }

        private _FileStatus GetStatus(string path)
        {
            string url = String.Format(QueryUrl.WebHdfs, path.TrimStart('/')) + QueryUrl.Status;

            Func<string, string, _FileStatus> action = (_url, _method) =>
            {
                var response = SendHdfsRequest(_url, _method);
                StreamReader sr = new StreamReader(response.GetResponseStream());
                FileStatusResponse fsr = JsonConvert.DeserializeObject<FileStatusResponse>(sr.ReadToEnd());
                sr.Close();
                response.Close();
                return fsr.FileStatus;
            };

            return webAction.Retry(url, WebRequestMethods.Http.Get, action, ExceptionTrigger);
        }

        private bool DownloadFile(string remotePath, string localPath)
        {
            string url = String.Format(QueryUrl.WebHdfs, remotePath.TrimStart('/')) + QueryUrl.Open;

            Func<string, string, bool> action = (_url, _method) =>
            {
                using (WebClient wc = new WebClient())
                {
                    wc.DownloadFile(FormatNameNode(_url), localPath);
                }
                return true;
            };

            return webAction.Retry(url, null, action, ExceptionTrigger);
        }

        private bool DownloadDirectory(string remotePath, string localPath)
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

                foreach (var fileName in listResult.Files)
                {
                    DownloadFile(CombineUrl(remotePath, pathSuffix, fileName),
                                 CombineUrl(localPath, pathSuffix, fileName));
                }

                foreach (var dirName in listResult.Dirs)
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
                CheckAndRemoveTargetData(localPath);
                File.Move(localTmpPath, localPath);
            }
            else
            {
                CheckAndRemoveTargetData(localTmpPath);
            }
            return success;
        }

        private bool DownloadByTmpDirectory(string remotePath, string localPath)
        {
            string localTmpPath = GenerateTmpPath(localPath);
            bool success = DownloadDirectory(remotePath, localTmpPath);
            if (success)
            {
                CheckAndRemoveTargetData(localPath);
                Directory.Move(localTmpPath, localPath);
            }
            else
            {
                CheckAndRemoveTargetData(localTmpPath);
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

        public bool Download(string remotePath, string localPath)
        {
            return Download(remotePath, localPath, true);
        }

        public bool Download(string remotePath, string localPath, bool overwrite)
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