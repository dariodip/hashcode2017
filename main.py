from cache import mmm, loader
import os

if __name__ == '__main__':

    fname = 'resources/trending_today.in'
    caches, endpoints, video_reqz, vl, bomba, min_video_size = loader.load(os.path.join(os.path.curdir, fname))

    inserted_videos_dict = {i: set() for i in range(len(endpoints))}
    while not video_reqz.empty():
        rr = video_reqz.get()
        most_requested_video, requiring_ep, req_times = rr[1].my_dict['vid'],rr[1].my_dict['epid'], rr[0]
        if int(vl[most_requested_video]) > bomba['cs']:
            continue
        ep = endpoints[requiring_ep]
        buff_caches = list()
        while not ep.caches.empty():
            cc = ep.caches.get()
            cache_lat = cc[0]
            our_cache = cc[1]
            if cache_lat >= ep.ep_lat:
                break
            if our_cache.remaining_size >= int(vl[most_requested_video]):
                if not most_requested_video in our_cache.inserted_videos \
                        and not most_requested_video in inserted_videos_dict[requiring_ep]:
                    our_cache.insert_video({'id': most_requested_video, 'size': vl[most_requested_video]})
                    inserted_videos_dict[requiring_ep].add(most_requested_video)
            if our_cache.remaining_size >= min_video_size:
                buff_caches.append(cc)
        for i in buff_caches:
            ep.caches.put(i)

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