using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace IoTHubScaling
{
    public static class IoTHubScaling
    {
        // We would be using a name instance for our orchestrator
        const string IotHubScaleOrchestratorInstanceId = "IotHubScaleOrchestrator_1";

        [FunctionName(nameof(IotHubScaleInit))]
        public static async Task IotHubScaleInit(
          [TimerTrigger("0 0 * * * *")]TimerInfo myTimer,
          [DurableClient] IDurableOrchestrationClient starter,
          ILogger log)
        {
            log.LogInformation($"Timer trigger started at: {DateTime.Now}");

            // check and see if a named instance of the orchestrator is already running
            var existingInstance = await starter.GetStatusAsync(IotHubScaleOrchestratorInstanceId);
            if (existingInstance == null)
            {
                log.LogInformation(String.Format("No instnace of job is running, starting new instance...", IotHubScaleOrchestratorInstanceId));
                await starter.StartNewAsync(nameof(IotHubScaleOrchestrator), IotHubScaleOrchestratorInstanceId);
            }
            else
                log.LogInformation(String.Format("Another instance already running, nothing to do..."));
        }

        [FunctionName(nameof(IotHubScaleOrchestrator))]
        public static async Task IotHubScaleOrchestrator(
                [OrchestrationTrigger] IDurableOrchestrationContext context,
                ILogger log)
        {
            log.LogInformation("IotHubScaleOrchestrator started");

            // launch and wait on the "worker" function
            await context.CallActivityAsync(nameof(IotHubScaleWorker), null);

            // register a timer with the durable functions infrastructure to re-launch the orchestrator in the future
            DateTime wakeupTime = context.CurrentUtcDateTime.Add(TimeSpan.FromMinutes(JobFrequencyMinutes));
            await context.CreateTimer(wakeupTime, CancellationToken.None);

            log.Info(String.Format("IotHubScaleOrchestrator done...  tee'ing up next instance in {0} minutes.", JobFrequencyMinutes.ToString()));

            // end this 'instance' of the orchestrator and schedule another one to start based on the timer above
            context.ContinueAsNew(null);
        }

        // worker function - does the actual work of scaling the IoTHub
        [FunctionName(nameof(IotHubScaleWorker))]
        public static void IotHubScaleWorker(
            [ActivityTrigger] DurableActivityContext context,
            TraceWriter log)
        {
            // connect management lib to iotHub
            IotHubClient client = GetNewIotHubClient(log);
            if (client == null)
            {
                log.Error("Unable to create IotHub client");
                return;
            }

            // get IotHub properties, the most important of which for our use is the current Sku details
            IotHubDescription desc = client.IotHubResource.Get(ResourceGroupName, IotHubName);
            string currentSKU = desc.Sku.Name;
            long currentUnits = desc.Sku.Capacity;

            // get current "used" message count for the IotHub
            long currentMessageCount = -1;
            IPage<IotHubQuotaMetricInfo> mi = client.IotHubResource.GetQuotaMetrics(ResourceGroupName, IotHubName);
            foreach (IotHubQuotaMetricInfo info in mi)
            {
                if (info.Name == "TotalMessages")
                    currentMessageCount = (long)info.CurrentValue;
            }
            if (currentMessageCount < 0)
            {
                log.Error("Unable to retreive current message count for IoTHub");
                return;
            }

            // compute the desired message threshold for the current sku
            long messageLimit = GetSkuUnitThreshold(desc.Sku.Name, desc.Sku.Capacity, ThresholdPercentage);

            log.Info("Current SKU Tier: " + desc.Sku.Tier);
            log.Info("Current SKU Name: " + currentSKU);
            log.Info("Current SKU Capacity: " + currentUnits.ToString());
            log.Info("Current Message Count:  " + currentMessageCount.ToString());
            log.Info("Current Sku/Unit Message Threshold:  " + messageLimit);

            // if we are below the threshold, nothing to do, bail
            if (currentMessageCount < messageLimit)
            {
                log.Info(String.Format("Current message count of {0} is less than the threshold of {1}. Nothing to do", currentMessageCount.ToString(), messageLimit));
                return;
            }
            else
                log.Info(String.Format("Current message count of {0} is over the threshold of {1}. Need to scale IotHub", currentMessageCount.ToString(), messageLimit));

            // figure out what new sku level and 'units' we need to scale to
            string newSkuName = desc.Sku.Name;
            long newSkuUnits = GetScaleUpTarget(desc.Sku.Name, desc.Sku.Capacity);
            if (newSkuUnits < 0)
            {
                log.Error("Unable to determine new scale units for IoTHub (perhaps you are already at the highest units for a tier?)");
                return;
            }

            // update the IoT Hub description with the new sku level and units
            desc.Sku.Name = newSkuName;
            desc.Sku.Capacity = newSkuUnits;

            // scale the IoT Hub by submitting the new configuration (tier and units)
            DateTime dtStart = DateTime.Now;
            client.IotHubResource.CreateOrUpdate(ResourceGroupName, IotHubName, desc);
            TimeSpan ts = new TimeSpan(DateTime.Now.Ticks - dtStart.Ticks);

            log.Info(String.Format("Updated IoTHub {0} from {1}-{2} to {3}-{4} in {5} seconds", IotHubName, currentSKU, currentUnits, newSkuName, newSkuUnits, ts.Seconds));

            //  this would be a good place to send notifications that you scaled up the hub :-)
        }

    }
}