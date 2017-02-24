from queue import PriorityQueue
from readytogo.cache import mmm
from itertools import groupby


def load(file):
    video_list = list()
    with open(file, 'r') as f:
        v, e, r, c, cs = [int(_) for _ in f.readline().split(' ')]
        line = f.readline().split(' ')
        video_list = {i: int(j) for i,j in zip(range(len(line)), line)}
        minimum_video_size = min([int(video_list[_]) for _ in video_list])
        if not len(video_list) == v:
            raise Exception("Ma cosa?")

        caches = {i: mmm.Cache(cs) for i in range(c)}
        endpoints = {}
        video_reqs = PriorityQueue()
        for i in range(e):
            latency1, n_cache = [int(_) for _ in f.readline().split(' ')]
            first_pq = PriorityQueue()
            for j in range(n_cache):
                cache_id, cache_lat = [int(_) for _ in f.readline().split(' ')]
                first_pq.put((cache_lat, caches[cache_id]))
            ep = mmm.EndPoint(first_pq, latency1)
            endpoints[i] = ep

        for i in range(r):
            video_id, end_point_id, request_times = [int(_) for _ in f.readline().split(' ')]
            if video_list[video_id] > cs:
                continue
            vid = mmm.Video(video_list[video_id], {'vid':video_id, 'epid':end_point_id})
            video_reqs.put((-request_times, vid))

        if not len(endpoints) == e:
            raise Exception("Ma cosa?")
        if not len(caches) == c:
            raise Exception("Ma cosa?")

        return caches, endpoints,video_reqs, video_list, {'v': v, 'e': e, 'r': r, 'c': c, 'cs': cs}, minimum_video_size

