from cache import cache_objects as co
from cache import loader
import os
import logging

log = logging.getLogger("main")


def main(fname):

    """
    caches:  dict {cache_id : cache_obj}
    endpoints: dict {endpoint_id : endpoint_obj}
    video_reqs: Priority Queue {key: -requests times; value: (video_obj)}
    video_size: dict {video_id: video_size}
    load_info: dict {v: video_count, e: endpoint_count, r: requests_count, c:caches_count, cs:cache_size}
    min_video_size: minimum size of all videos (threshold)
    """
    caches, endpoints, video_reqs, video_size, load_info, min_video_size = \
        loader.load(os.path.join(os.path.curdir, fname))
    # a dict that stores, for each video, the cache in which it was addes (to avoid duplicates)
    inserted_videos_sets = {i: set() for i in range(len(endpoints))}
    while not video_reqs.empty():  # starting from most requested video until the queue was empty
        video_request = video_reqs.get()  # get most requested video's object
        video_obj = video_request[1]  # dict {'vid': video_id, 'epid': requiring_endpoint_id}
        most_requested_video_id, requiring_ep, req_times = video_obj.video_id, video_obj.end_point_id, video_request[0]

        if video_size[most_requested_video_id] > load_info['cache_size']:  # this cannot happens, but...
            continue

        requesting_endpoint = endpoints[requiring_ep]
        caches_buffer = list()  # to reinsert still valid caches
        while not requesting_endpoint.caches.empty():  # try to insert in each cache starting from faster one
            faster_cache_entry = requesting_endpoint.caches.get()
            cache_latency = faster_cache_entry[0]
            cache = faster_cache_entry[1]
            if cache_latency >= requesting_endpoint.latency_to_datacenter:  # it is worth to not insert in cache
                break
            if cache.remaining_size >= video_size[most_requested_video_id]:
                if most_requested_video_id not in cache.inserted_videos \
                        and most_requested_video_id not in inserted_videos_sets[requiring_ep]:
                    cache.insert_video({'id': most_requested_video_id, 'size': video_size[most_requested_video_id]})
                    inserted_videos_sets[requiring_ep].add(most_requested_video_id)
            if cache.remaining_size >= min_video_size:  # enough space for at least another video
                caches_buffer.append(faster_cache_entry)
        for i in caches_buffer:  # reinsert in the queue
            requesting_endpoint.caches.put(i)

    new_cache = dict()
    k = 0
    for i in caches:
        if not len(caches[i].inserted_videos) == 0:
            new_cache[k] = caches[i]
            k += 1

    with open(fname + ".solution", 'w+') as ff:
        ff.write(str(len(new_cache)) + "\n")
        for i in new_cache:
            ff.write(str(i) + ' ' + "".join(str(_) + " " for _ in new_cache[i].inserted_videos) + "\n")

if __name__ == '__main__':
    log.warning("Main started!")
    for file in filter(lambda f: f.endswith('.in'), os.listdir('./resources')):
        log.warning("Working on {}".format(file))
        main('./resources/' + file)
        log.warning("{} ends".format(file))
