import os
import sys

import oss2


def main(key, filename):
    auth = oss2.Auth('accessId', 'accessSecret')
    bucket = oss2.Bucket(auth, 'endpoint.aliyuncs.com', 'bucket')

    total_size = os.path.getsize(filename)
    part_size = oss2.determine_part_size(total_size, preferred_size=100 * 1024)

    # init
    upload_id = bucket.init_multipart_upload(key).upload_id
    parts = []

    print 'start to upload {} with id {}'.format(filename, upload_id)

    # upload
    with open(filename, 'rb') as fileobj:
        part_number = 1
        offset = 0
        while offset < total_size:
            num_to_upload = min(part_size, total_size - offset)
            result = bucket.upload_part(key, upload_id, part_number,
                                        oss2.SizedFileAdapter(fileobj, num_to_upload))
            parts.append(oss2.models.PartInfo(part_number, result.etag))

            offset += num_to_upload
            part_number += 1

    # complete
    bucket.complete_multipart_upload(key, upload_id, parts)
    print 'done!'


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'argument: key, filename'
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])

