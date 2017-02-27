from cache import loader
import os
import logging

log = logging.getLogger("main")


# Prettify output
def prettify_cache(caches):
    new_cache = dict()
    k = 0
    for i in caches:
        if not len(caches[i].inserted_videos) == 0:
            new_cache[k] = caches[i]
            k += 1
    return new_cache


# Algorithm
def main(fname):

    """
    caches:  dict {cache_id : cache_obj}
    endpoints: dict {endpoint_id : endpoint_obj}
    video_reqs: Priority Queue {key: -requests times; value: (video_obj)}
    video_size: dict {video_id: video_size}
    load_info: dict {v: video_count, e: endpoint_count, r: requests_count, c:caches_count, cs:cache_size}
    min_video_size: minimum size of all videos (threshold)
    """
    caches, endpoints, video_reqs, video_sizes, load_info, min_video_size = \
        loader.load(os.path.join(os.path.curdir, fname))
    # a dict that stores, for each video, the cache in which it was added (to avoid duplicates)
    inserted_videos_sets = {i: set() for i in range(len(endpoints))}
    while not video_reqs.empty():  # starting from most requested video until the queue was empty
        video_request = video_reqs.get()  # get most requested video's object
        # video_obj: dict {'vid': video_id, 'end_point_id': requiring_endpoint_id}
        # req_times: priority
        video_obj, req_times = video_request[1], video_request[0]
        most_requested_video_id, requiring_ep = video_obj.video_id, video_obj.end_point_id

        # Video size
        current_video_size = video_sizes[most_requested_video_id]

        if current_video_size > load_info['cache_size']:  # this cannot happen, but...
            print("Happened...")
            continue

        # Get the Endpoint that is requesting the current video
        requesting_endpoint = endpoints[requiring_ep]
        caches_buffer = list()  # to reinsert still valid caches
        while not requesting_endpoint.caches.empty():  # try to insert in each cache starting from faster one
            faster_cache_entry = requesting_endpoint.caches.get()
            current_cache_latency = faster_cache_entry[0]
            current_cache = faster_cache_entry[1]
            if current_cache_latency >= requesting_endpoint.latency_to_datacenter:  # not worth adding in cache
                break
            if current_cache.remaining_size >= current_video_size:  # if there is enough space in the cache
                if most_requested_video_id not in inserted_videos_sets[requiring_ep] and \
                most_requested_video_id not in current_cache.inserted_videos:
                    current_cache.insert_video({'id': most_requested_video_id, 'size': current_video_size})
                    inserted_videos_sets[requiring_ep].add(most_requested_video_id)
            if current_cache.remaining_size >= min_video_size:  # enough space for at least another video
                caches_buffer.append(faster_cache_entry)
        for i in caches_buffer:  # reinsert in the queue
            requesting_endpoint.caches.put(i)

    # Prettify the solution
    new_cache = prettify_cache(caches)

    with open(fname + ".solution", 'w+') as ff:
        ff.write(str(len(new_cache)) + "\n")
        for i in new_cache:
            ff.write(str(i) + ' ' + "".join(str(_) + " " for _ in new_cache[i].inserted_videos) + "\n")


# Main function
if __name__ == '__main__':
    log.warning("Main started!")
    for file in filter(lambda f: f.endswith('.in'), os.listdir('./resources')):
        log.warning("Working on {}".format(file))
        main('./resources/' + file)
        log.warning("{} ends".format(file))