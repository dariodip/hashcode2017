from cache import loader
import os
import logging
from cache.cache_objects import Score

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

    all_scores = list()

    while not video_reqs.empty():  # starting from most requested video until the queue was empty
        video_request = video_reqs.get()  # get most requested video's object
        # video_obj: dict {'vid': video_id, 'end_point_id': requiring_endpoint_id}
        # req_times: priority
        req_obj, req_times = video_request[1], -video_request[0]
        # Video size
        current_video_size = req_obj.required_video.size

        if current_video_size > load_info['cache_size']:  # this cannot happen, but...
            print("Happened...")
            continue
        if len(req_obj.requiring_ep) == 0:  # this cannot happen, but...
            print("Happened...")
            continue

        for cache_k in caches:
            cache = caches[cache_k]
            if cache.remaining_size < req_obj.required_video.size:
                continue
            cache_score = 0
            for ep in req_obj.requiring_ep:
                if cache.cache_id in ep.caches_dict:
                    cache_score += (req_times * (ep.latency_to_datacenter - ep.caches_dict[cache.cache_id]))
            if cache_score > 0:
                all_scores.append(Score(cache_score, req_obj.required_video, cache))

    sorted_scores = sorted(all_scores, key=lambda x: x.score, reverse=True)
    for attempt in sorted_scores:
        score, video, cache = attempt.score, attempt.video, attempt.cache
        if cache.remaining_size < video.size:
            continue
        if video.video_id in cache.inserted_videos:
            continue
        cache.insert_video(video)


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