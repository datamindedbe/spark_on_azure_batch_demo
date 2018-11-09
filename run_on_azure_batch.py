import datetime
import sys
import time

import azure.batch.batch_auth as batch_auth
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels

import config

sys.path.append('.')
sys.path.append('..')

IMAGE_NAME = 'datamindeddemo.azurecr.io/datamindeddemo/spark_on_azure_batch_demo'
IMAGE_VERSION = 'latest'


def create_pool(batch_service_client, container_registry, image_name, pool_id, pool_vm_size, pool_node_count,
                skip_if_exists=True):
    print('Creating pool [{}]...'.format(pool_id))

    container_conf = batch.models.ContainerConfiguration(
        container_image_names=[image_name],
        container_registries=[container_registry]
    )

    image_ref_to_use = batch.models.ImageReference(
        publisher='microsoft-azure-batch',
        offer='ubuntu-server-container',
        sku='16-04-lts',
        version='latest')

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batch.models.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            container_configuration=container_conf,
            node_agent_sku_id='batch.node.ubuntu 16.04'),
        vm_size=pool_vm_size,
        target_low_priority_nodes=pool_node_count)

    if not skip_if_exists or not batch_service_client.pool.exists(pool_id):
        batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, job_id, pool_id):
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_task(batch_service_client, image_name, image_version, job_id, command, name):
    user = batchmodels.UserIdentity(
        auto_user=batchmodels.AutoUserSpecification(
            elevation_level=batchmodels.ElevationLevel.admin,
            scope=batchmodels.AutoUserScope.task))

    task_id = name
    task_container_settings = batch.models.TaskContainerSettings(
        image_name=image_name + ':' + image_version,
        container_run_options='--rm -p 4040:4040')
    task = batch.models.TaskAddParameter(
        id=task_id,
        command_line=command,
        container_settings=task_container_settings,
        user_identity=user
    )
    print("running " + command)

    batch_service_client.task.add(job_id, task)


def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    timeout_expiration = datetime.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Start batch job {}'.format(start_time))
    print()

    image_name = IMAGE_NAME
    image_version = IMAGE_VERSION

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    credentials = batch_auth.SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
                                                  config.BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(credentials, base_url=config.BATCH_ACCOUNT_URL)

    job_id = 'Job-' + '{:%Y-%m-%d-%H-%M-%S}'.format(datetime.datetime.now())
    pool_id = 'Airlines'

    try:
        container_registry = batch.models.ContainerRegistry(
            registry_server=config.ACR_LOGINSERVER,
            user_name=config.ACR_USERNAME,
            password=config.ACR_PASSWORD)

        create_pool(
            batch_service_client=batch_client,
            pool_id=pool_id,
            container_registry=container_registry,
            image_name=image_name,
            pool_node_count=3,
            pool_vm_size='Standard_E2s_v3',
            skip_if_exists=True)

        # Create the job that will run the tasks.
        create_job(batch_client, job_id, pool_id)

        for year in range(2001, 2009):
            command = "python /src/airline_analytics.py --input wasbs://demo@datamindeddata.blob.core.windows.net/raw/airlines/{0}.csv.bz2 --output wasbs://demo@datamindeddata.blob.core.windows.net/aggregated/airlines/{1}.parquet" \
                .format(year, year)
            add_task(
                batch_service_client=batch_client,
                image_name=image_name,
                image_version=image_version,
                job_id=job_id,
                command=command,
                name='airlines{0}'.format(year))

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client, job_id, datetime.timedelta(hours=2))

        print("  Success! All tasks reached the 'Completed' state within the specified timeout period.")

    except batchmodels.BatchErrorException as err:
        print(err)
        raise

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()
    print()
