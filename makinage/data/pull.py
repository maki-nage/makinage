import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition


async def pull(loop, server, topic, group_id, batch_size=1, shuffle=False):
    client = AIOKafkaConsumer(
        topic,
        loop=loop,
        bootstrap_servers=server,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )
    await client.start()

    partitions = client.partitions_for_topic(topic)
    while partitions is None:
        await asyncio.sleep(0.1)

    partitions = list(partitions)
    partitions = [TopicPartition(topic, partition) for partition in partitions]
    #current_offsets = await client.beginning_offsets(partitions)
    end_offsets = await client.end_offsets(partitions)
    current_partition = 0
    done = False

    async def next_partition(current_partition):
        current_partition += 1  # todo recursive
        if current_partition >= len(partitions):
            return None

        current_offset = await client.position(partitions[current_partition])
        if current_offset >= end_offsets[partitions[current_partition]]:
            current_partition = await next_partition(current_partition)
        print("remaining record: {}, partition: {}".format(remaining_records, current_partition))
        return current_partition

    current_offset = await client.position(partitions[current_partition])
    if current_offset >= end_offsets[partitions[current_partition]]:
        done = True

    while done is False:
        remaining_records = batch_size
        batch = []
        while remaining_records > 0:
            msg = await client.getone(partitions[current_partition])
            batch.append(msg)
            remaining_records -= 1

            current_offset = await client.position(partitions[current_partition])
            if current_offset >= end_offsets[partitions[current_partition]]:
                current_partition = await next_partition(current_partition)
                print("remaining record: {}, partition: {}".format(remaining_records, current_partition))
                if current_partition is None:
                    done = True
                    break

        if len(batch) > 0:
            yield(batch)

        '''
        data = await client.getmany(max_records=batch_size)
        print(data)
        #for tp, messages in data.items():
        messages = data[topic]
        if len(messages) > 0:
            batch = []
            for msg in messages:
                batch.append(msg)
            yield(batch)
        else:
            done = True
        '''

    await client.stop()
