import luigi
from luigi import ExternalTask, Parameter, Task, LocalTarget, BoolParameter
from .luigi import ContentImage, DownloadImage
from luigi.contrib.external_program import ExternalProgramTask
from .target import SuffixPreservingLocalTarget, BaseAtomicProviderLocalTarget, suffix_preserving_atomic_file
from .tasks import Requires, Requirement
import os
from os.path import split, join, splitext
import glob
import dask_image
from dask_image.imread import imread
import cv2 as cv2
from PIL import Image
import dask.array as da
import h5py



class OutputStorage(Task):

    LOCAL_ROOT = os.path.abspath("data")
    SHARED_RELATIVE_PATH = "storage"
    subset = BoolParameter(default=True)
    requires = Requires()
    reviews = Requirement(ContentImage)


    def output(self):

        return SuffixPreservingLocalTarget(self.LOCAL_ROOT + "/" + self.SHARED_RELATIVE_PATH+"/")

    def run(self):
        os.makedirs(self.output().path)
        filename_pattern = os.path.join('./data/images', '*.jpg')

        dsk_images = dask_image.imread.imread(filename_pattern)

        da.to_hdf5('data/storage'+"/"+'stored.hdf5', {'/x': dsk_images[0]})



class ProcessImages(Task):
    subset = BoolParameter(default=True)
    requires=Requires()
    cleaned_review=Requirement(DownloadImage)
    LOCAL_ROOT = os.path.abspath("data")
    SHARED_RELATIVE_PATH = "small"
    height = 100
    width = 100

    def requires(self):
        """:param image - requires DownloadImage function from data file
        :returns image and model dict"""
        return {"image": DownloadImage()}

    def output(self):

        return SuffixPreservingLocalTarget(self.LOCAL_ROOT + "/" + self.SHARED_RELATIVE_PATH+"/")

    def run(self):

        os.makedirs(self.output().path)
        for img in glob.glob("data/images/*.jpg"):
         image_pil = Image.open(img, 'r')
         ratio_w = self.width / image_pil.width
         ratio_h = self.height / image_pil.height
         if ratio_w < ratio_h:
         # It must be fixed by width
            resize_width = self.width
            resize_height = round(ratio_w * image_pil.height)
         else:
        # Fixed by height
             resize_width = round(ratio_h * image_pil.width)
             resize_height = self.height
         image_resize = image_pil.resize((resize_width, resize_height), Image.ANTIALIAS)
         background = Image.new('RGB', (self.width, self.height), (255,255, 255))
         background.paste(image_resize, (round((self.width - resize_width) / 2), round((self.height - resize_height) / 2)))
         with self.output().atomic_provider(self.output().path+img.split('/')[-1]) as outfile:
            background.save(outfile, "JPEG", quality=80, optimize=True, progressive=True)





#### Segmenting

#After the image preprocessing, we segment regions of interest from the data.
# We’ll use a simple arbitrary threshold as the cutoff, at 75% of the maximum intensity of the smoothed image.



class BySegment(Task):
    LOCAL_ROOT = os.path.abspath("data")
    SHARED_RELATIVE_PATH = "output"
    subset = BoolParameter(default=True)
    requires = Requires()
    reviews = Requirement(ProcessImages)

    def output(self):
       return SuffixPreservingLocalTarget(self.LOCAL_ROOT + "/" + self.SHARED_RELATIVE_PATH, format=luigi.format.Nop)

    def run(self):

        os.makedirs(self.output().path)

        for img in glob.glob("data/images/*.jpg"):
           # cv_img = cv2.imread(img)
            gray = cv2.cvtColor(cv2.imread(img), cv2.COLOR_BGR2GRAY);
            binary = cv2.adaptiveThreshold(cv2.GaussianBlur(gray,(5,5),0), 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,cv2.THRESH_BINARY,11,2)
            output = self.output().path +"/"+img.split('/')[-1]
            cv2.imwrite(output, binary)




        #for img in glob.glob("data/images/*.jpg"):
       # dsk_images = dask_image.imread.imread(glob.glob("data/images/*.jpg"))
      #  print(dsk_images.shape)

      #  threshold_value = 0.75 * da.max(dsk_images[0:3]).compute()
      #  print(threshold_value)
