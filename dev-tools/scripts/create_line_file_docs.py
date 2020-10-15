import os
import gzip
import time
import random
import re
import urllib.request
import subprocess
import tempfile
import shutil

DEBUG = False

TARGET_DOC_CHARS = 1024

def compress_with_seek_points(file_name_in, file_name_out, num_seek_points):

  bytes_per_chunk = os.path.getsize(file_name_in) / num_seek_points

  seek_points = []

  if os.path.exists(file_name_out):
    os.remove(file_name_out)

  with open(file_name_in, 'rb') as f_in:

    f_out = None

    bytes_in_chunk = 0

    chunk_count = 0

    while True:
      if f_out is None:
        if os.path.exists(file_name_out):
          seek_points.append(os.path.getsize(file_name_out))
          print('  create chunk %s at pos=%s' % (chunk_count, seek_points[-1]))
        else:
          print('  create chunk %s at pos=0' % chunk_count)
        f_out = gzip.open(file_name_out, 'ab')
        chunk_count += 1

      line = f_in.readline()
      if len(line) == 0:
        break

      bytes_in_chunk += len(line)
      f_out.write(line)

      if bytes_in_chunk > bytes_per_chunk and chunk_count < num_seek_points:
        f_out.close()
        f_out = None
        bytes_in_chunk = 0

  with open(file_name_out[:-3] + '.seek', 'w') as f_out:
    for seek_point in seek_points:
      f_out.write('%d\n' % seek_point)

re_tag = re.compile('<[^>]+?>')
re_newlines = re.compile('\n+')
re_space = re.compile('\s')

# used to find word break, for splitting docs into ~1 KB sized smaller docs:
re_next_non_word_character = re.compile('\W', re.U)

EUROPARL_V7_URL = 'https://www.statmt.org/europarl/v7/europarl.tgz'

def split_docs(all_out, title_string, date_string, body_string):

  '''
  Splits docs into smallish (~1 KB) sized docs, repeating same title and date
  '''

  doc_count = 0
  while len(body_string) > 0:
    char_count = int(random.gauss(TARGET_DOC_CHARS, TARGET_DOC_CHARS/4))
    if char_count < 64:
      # trimmed normal?
      continue

    m = re_next_non_word_character.search(body_string, char_count)
    if m is not None:
      char_count = m.start(0)
    else:
      char_count = len(body_string)

    body_string_fragment = body_string[:char_count].strip()
    
    #print('write title %d, body %d' % (len(title_string), len(body_string_fragment)))
    all_out.write('%s\t%s\t%s\n' % (title_string, date_string, body_string_fragment))
    body_string = body_string[char_count:]
    doc_count += 1

  return doc_count

def sample_europarl():

  # download europarl.tgz v7, if not already here (in cwd):
  file_name = 'europarl.tgz'
  if not os.path.exists(file_name):
    print('Download %s to %s...' % (EUROPARL_V7_URL, file_name))
    urllib.request.urlretrieve(EUROPARL_V7_URL, file_name + '.tmp')
    os.rename(file_name + '.tmp', file_name)
  else:
    print('%s already here; skipping download...' % file_name)

  if not DEBUG:
    tmp_dir_path = tempfile.mkdtemp()
  else:
    tmp_dir_path = '/tmp/tmp31ekzg75'
  print('Using tmp dir "%s"...' % tmp_dir_path)
  try:
    if not DEBUG:
      cmd = 'tar xzf %s -C %s' % (file_name, tmp_dir_path)
      print('Run: %s' % cmd)
      subprocess.run(cmd, shell=True)

    doc_count = 0
    skip_count = 0
    file_count = 0

    all_txt_file_name = '%s/all.txt' % tmp_dir_path

    print('Extract text...')

    start_time = time.time()
    next_print_time = start_time + 3
    # normalize text a bit and concatenate all lines into single file, counting total lines/bytes
    with open(all_txt_file_name, 'w', encoding='utf-8') as all_out:
      for dir_path, dir_names, file_names in os.walk('%s/txt' % tmp_dir_path):
        for file_name in file_names:
          if file_name.endswith('.txt'):
            file_count += 1

            year, month, day = (int(x) for x in file_name[3:-4].split('-')[:3])
            if year >= 50:
              year = 1900 + year
            else:
              year = 2000 + year

            date_string = '%04d-%02d-%02d' % (year, month, day)
            
            # unfortunately we need errors='ignore' since in Europarl v7, one file (pl/ep-09-10-22-009.txt) has invalid utf-8:
            chapter_count = 0
            with open('%s/%s' % (dir_path, file_name), 'r', encoding='utf-8', errors='ignore') as f_in:
              last_text = []
              last_title = None
              while True:
                line = f_in.readline()
                if line == '':
                  break
                line = line.strip()
                if line.startswith('<CHAPTER '):
                  if last_title is not None:
                    s = ' '.join(last_text)
                    s = re_tag.sub(' ', s)
                    s = re_newlines.sub(' ', s)
                    s = s.strip()
                    if len(s) > 0:
                      doc_count += split_docs(all_out, last_title, date_string, s)
                    else:
                      skip_count += 1
                      
                    last_text = []
                    chapter_count += 1
                  while True:
                    last_title = f_in.readline()
                    if last_title == '':
                      last_title = None
                      break
                    last_title = re_tag.sub(' ', last_title).strip()
                    if len(last_title) > 0:
                      break
                  continue
                else:
                  last_text.append(line)

              if last_title is not None:
                s = ' '.join(last_text)
                s = re_tag.sub(' ', s)
                s = re_newlines.sub(' ', s)
                s = s.strip()
                if len(s) > 0:
                  doc_count += split_docs(all_out, last_title, date_string, s)
                else:
                  skip_count += 1
                chapter_count += 1
              else:
                skip_count += 1

              if chapter_count > 0:
                #print('%s/%s: %d chapters' % (dir_path, file_name, chapter_count))
                pass

            now = time.time()
            if now > next_print_time:
              print('%4.1fs: keep %.2f K of %.2f K files (%.1f%%), %.2f M docs, %.2f GB...' % \
                    (now - start_time, (file_count - skip_count) / 1000, file_count / 1000,
                     100 * (file_count - skip_count) / file_count,
                     doc_count / 1000000, all_out.tell() / 1024/1024/1024))
              while next_print_time < now:
                next_print_time += 3

    total_mb = os.path.getsize(all_txt_file_name)/1024/1024
    now = time.time()
    print('%4.1fs (done): keep %.2f K of %.2f K files (%.1f%%), %.2f M docs, %.2f GB...' % \
          (now - start_time, (file_count - skip_count) / 1000, file_count / 1000,
           100 * (file_count - skip_count) / file_count,
           doc_count / 1000000, os.path.getsize(all_txt_file_name) / 1024/1024/1024))

    print('Shuffle...')
    subprocess.run('shuf %s > %s.shuffled' % (all_txt_file_name, all_txt_file_name), shell=True)

    for mb in (20, 200, 2000):
      print('Sample %d MB file...' % mb)
      file_name_out = '%dmb.txt' % mb
      with open(file_name_out, 'w', encoding='utf-8') as f_out:

        chance = mb / total_mb

        with open(all_txt_file_name + '.shuffled', 'r', encoding='utf-8') as f:

          while True:
            line = f.readline()
            if len(line) == 0:
              break
            if random.random() <= chance:
              f_out.write(line)

      print('  got %.2f MB' % (os.path.getsize(file_name_out)/1024/1024))

      compress_with_seek_points(file_name_out,
                                file_name_out + '.gz',
                                mb)
            
  finally:
    print('Removing tmp dir "%s"...' % tmp_dir_path)
    if not DEBUG:
      shutil.rmtree(tmp_dir_path)

  print('\nWARNING: left ./europarl.tgz, which you should delete if you do not want it!\n')

if False:
  compress_with_seek_points('/x/tmp/europarl.lines.txt',
                            '/x/tmp/foo.txt.gz',
                            16)
else:
  sample_europarl()
