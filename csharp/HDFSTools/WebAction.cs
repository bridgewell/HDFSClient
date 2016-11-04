using System;
using System.Net;

namespace HDFSTools
{
    class WebAction
    {
        private const int DefaultErrorBoundary = 3;
        private Action<string, WebException, bool> defaultExceptionTrigger = EmptyTrigger;

        private static void EmptyTrigger(string url, WebException we, bool isLast)
        {
            // Do nothing
        }

        public void SetDefaultExceptionTrigger(Action<string, WebException, bool> trigger)
        {
            this.defaultExceptionTrigger = trigger;
        }

        public T Retry<T>(string url, string method, Func<string, string, T> action, Action<string, WebException, bool> errTrigger = null, int errBoundary = DefaultErrorBoundary)
        {
            if (errTrigger == null)
            {
                errTrigger = this.defaultExceptionTrigger;
            }

            int errCount = 0;
            T result = default(T);
            while (errCount < errBoundary)
            {
                try
                {
                    result = action(url, method);
                    break;
                }
                catch (WebException ex)
                {
                    errCount++;
                    if (errCount >= errBoundary)
                    {
                        errTrigger(url, ex, true);
                    }
                    errTrigger(url, ex, false);
                }
            }
            return result;
        }
    }
}