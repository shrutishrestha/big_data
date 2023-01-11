import os
import utils 
import rasterio
import findspark
import numpy as np
import pandas as pd
from enum import Enum
import geopyspark as gps
from typing import Callable
from typing import Iterator
from datetime import datetime
from pyspark import SparkContext
from abc import ABCMeta, abstractmethod
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from scipy.stats import skew, kurtosis, moment
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, BinaryType


class PatchSize(Enum):
    """
    This contains all possible sizes of a patch that parameters are going to be
    computed for.

    Note: For adding new items to the list, make sure that the input
    images can be divided by the new number. (bImage.getWidth % newItem == 0)

    """
    ONE = 1
    FOUR = 4
    SIXTEEN = 6
    THIRTY_TWO = 32
    SIXTY_FOUR = 64
    ONE_TWENTY_EIGHT = 128
    TWO_FIFTY_SIX = 256
    FIVE_TWELVE = 512
    TEN_TWENTY_FOUR = 1024
    FULL = -1
    
    

class BaseParamCalculator():
    """
    This is a base abstract class for calculating parameters over some patch of a 2D array.

    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            the patch size to calculate the parameter over.

        """
        if patch_size is None:
            raise TypeError("PatchSize cannot be None in ParamCalculator constructor.")
        self._patch_size = patch_size

    @property
    @abstractmethod
    def calc_func(self) -> Callable:
        """
        This polymorphic property is designed to return the parameter calculation function to be applied to each
        patch of the input data.

        :return: :py:class:`typing.Callable` that is the parameter calculation function over a patch of a 2D array.
        """
        pass

    def calculate_parameter(self, data: np.ndarray) -> np.ndarray:
        """
        This polymorphic method is designed to compute some image parameter. The parameters shall be calculated by
        iterating over a given 2D in a patch by patch manner, calculating the parameter for the pixel values within that
        patch.

        :param data: :py:class:`np.ndarray`
            2D matrix representing some image

        :return: either a :py:class:`np.ndarray` of the parameter value for each patch within the original input
            :py:class:`np.ndarray`, or a single value representing the parameter value of the entire input
            :py:class:`np.ndarray`.

        """
        if data is None or not isinstance(data, np.ndarray):
            raise TypeError("Data cannot be None and must be of type np.ndarray")
        if self._patch_size is None:
            raise TypeError("PatchSize cannot be None in calculator.")

        image_h = data.shape[0]
        image_w = data.shape[1]

        if self._patch_size is PatchSize.FULL:
            return self.calc_func(data)

        p_size = self._patch_size.value

        if image_w % p_size != 0:
            raise ValueError("Width of data must be divisible by given patch size!")
        if image_h % p_size != 0:
            raise ValueError("Height of data must be divisible by given patch size!")

        div_h = image_h // p_size
        div_w = image_w // p_size

        vals = np.zeros((int(div_h), int(div_w)))
        for row in range(div_h):
            for col in range(div_w):
                start_r = p_size * row
                end_r = start_r + p_size
                start_c = p_size * col
                end_c = start_c + p_size
                vals[row, col] = self.calc_func(data[start_r:end_r, start_c:end_c])

        return vals

class MeanParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the mean parameter over some patch of a 2D array.
    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            the patch size to calculate the parameter over.

        """

        super().__init__(patch_size)
        self._calc_func = np.mean

    @property
    def calc_func(self) -> Callable:
        return self._calc_func



class StdDeviationParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the standard deviation parameter over some patch of a 2D array.

    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            the patch size to calculate the parameter over.

        """
        super().__init__(patch_size)
        self._calc_func = np.std

    @property
    def calc_func(self) -> Callable:
        return self._calc_func


###################################
# SkewnessParamCalculator
####################################
class SkewnessParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the skewness parameter over some patch of a 2D array.

    See :py:class:`scipy.stats.skew` for additional information on the calculation for
    each cell.
    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            the patch size to calculate the parameter over.

        """
        super().__init__(patch_size)

    @staticmethod
    def _calc_func(data: np.ndarray) -> np.ndarray:
        val = skew(data, axis=None)
        return val

    @property
    def calc_func(self) -> Callable:
        return self._calc_func

###################################
# KurtosisParamCalculator
###################################
class KurtosisParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the kurtosis parameter over some patch of a 2D array.

    See :py:class:`scipy.stats.kurtosis` for additional information on the calculation for
    each cell.
    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            the patch size to calculate the parameter over.

        """
        super().__init__(patch_size)

    @staticmethod
    def _calc_func(data: np.ndarray) -> np.ndarray:
        val = kurtosis(data, axis=None)
        return val

    @property
    def calc_func(self) -> Callable:
        return self._calc_func


###################################
# RelativeSmoothnessParamCalculator
###################################
class RelativeSmoothnessParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the relative smoothness parameter over some patch of a 2D array.


    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            the patch size to calculate the parameter over.

        """
        super().__init__(patch_size)

    @staticmethod
    def _calc_func(data: np.ndarray) -> np.ndarray:
        val = np.var(data)
        val = 1 - (1.0 / (1 + val))
        return val

    @property
    def calc_func(self) -> Callable:
        return self._calc_func


###################################
# UniformityParamCalculator
###################################
class UniformityParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the uniformity parameter over some patch of a 2D array.


    """

    def __init__(self, patch_size: PatchSize, n_bins: int, min_val: float, max_val: float):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            The patch size to calculate the parameter over.
        :param n_bins: int or sequence of scalars or str
            The number of bins to use when constructing the frequency histogram for each patch.
            If bins is an int, it defines the number of equal-width bins in the given range.
            If bins is a sequence, it defines a monotonically increasing array of bin edges,
            including the rightmost edge, allowing for non-uniform bin widths. See :py:class:`np.histogram`
            as it is used internally
        :param min_val: py:float
            The minimum value to use when constructing the frequency histogram for each patch.
            Values outside the range are ignored
        :param max_val: float
            The maximum value to use when constructing the frequency histogram for each patch.
            Values outside the range are ignored. The max_val must be greater than or equal to min_val.

        """
        super().__init__(patch_size)

        if n_bins is None:
            raise TypeError("n_bins cannot be None in UniformityParamCalculator constructor.")
        if min_val is None:
            raise TypeError("min_val cannot be None in UniformityParamCalculator constructor.")
        if max_val is None:
            raise TypeError("max_val cannot be None in UniformityParamCalculator constructor.")

        if min_val > max_val:
            raise ValueError("max_val cannot be less than min_val in UniformityParamCalculator constructor.")

        self._n_bins = n_bins
        self._range = (min_val, max_val)

    @property
    def calc_func(self) -> Callable:
        return self.__calc_uniformity

    def __calc_uniformity(self, data: np.ndarray) -> float:
        """
        Helper method that performs the uniformity calculation for one patch.

        :param data: :py:class:`np.ndarray`
            2D matrix representing some image
        :return: The uniformity parameter value for the patch passed in

        """

        hist, bin_edges = np.histogram(data, self._n_bins, range=self._range)
        image_h = data.shape[0]
        image_w = data.shape[1]

        n_pix = float(image_w * image_h)
        sum = 0.0
        for i in range(len(hist)):
            count = hist[i]
            if count == 0:
                continue
            prob = hist[i] / n_pix
            sum += np.power(prob, 2)

        return sum

###################################
# EntropyParamCalculator
###################################
class EntropyParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the entropy parameter over some patch of a 2D array.
    The calculation is performed in the following manner:

    .. math:: E = - \\sum_{i=1}^{N} p(z_i)* log_2(p(z_i))

    where:

    - :math:`p` is the histogram of a patch

    - :math:`z_i` is the intensity value of the i-th pixel in the patch

    - :math:`p(z_i)` is the frequency of the intensity :math:`z_i` in the histogram of the patch

    """

    def __init__(self, patch_size: PatchSize, n_bins: int, min_val: float, max_val: float):
        """
        Constructor

        :param patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
            The patch size to calculate the parameter over.
        :param n_bins: int or sequence of scalars or str
            The number of bins to use when constructing the frequency histogram for each patch.
            If bins is an int, it defines the number of equal-width bins in the given range.
            If bins is a sequence, it defines a monotonically increasing array of bin edges,
            including the rightmost edge, allowing for non-uniform bin widths. See :py:class:`np.histogram`
            as it is used internally
        :param min_val: py:float
            The minimum value to use when constructing the frequency histogram for each patch.
            Values outside the range are ignored
        :param max_val: float
            The maximum value to use when constructing the frequency histogram for each patch.
            Values outside the range are ignored. The max_val must be greater than or equal to min_val.

        """
        super().__init__(patch_size)

        if n_bins is None:
            raise TypeError("n_bins cannot be None in EntropyParamCalculator constructor.")
        if min_val is None:
            raise TypeError("min_val cannot be None in EntropyParamCalculator constructor.")
        if max_val is None:
            raise TypeError("max_val cannot be None in EntropyParamCalculator constructor.")

        if min_val > max_val:
            raise ValueError("max_val cannot be less than min_val in EntropyParamCalculator constructor.")

        self._n_bins = n_bins
        self._range = (min_val, max_val)

    @property
    def calc_func(self) -> Callable:
        return self.__calc_entropy

    def __calc_entropy(self, data: np.ndarray) -> float:
        """
        Helper method that performs the entropy calculation for one patch.

        :param data: :py:class:`np.ndarray`
            2D matrix representing some image
        :return: The entropy parameter value for the patch passed in

        """

        hist, bin_edges = np.histogram(data, self._n_bins, range=self._range)
        image_h = data.shape[0]
        image_w = data.shape[1]

        n_pix = float(image_w * image_h)
        sum = 0.0
        for i in range(len(hist)):
            count = hist[i]
            if count == 0:
                continue
            prob = hist[i] / n_pix
            sum += prob * (np.log2(prob))

        return 0 - sum


###################################
# TContrastParamCalculator
###################################
class TContrastParamCalculator(BaseParamCalculator):
    """
    This class is for calculating the Tamura Contrast parameter over some patch of a 2D array.
    The calculation is performed in the following manner:

    .. math:: C = \\frac{\\sigma^{2}}{{\\mu_4}^{0.25}}

    where:

    - :math:`\\sigma^{2}` is the variance of the intensity values in the patch

    - :math:`\\mu_4` is kurtosis (4-th moment about the mean) of the intensity values in the patch

    This formula is an approximation proposed by Tamura et al. in "Textual Features Corresponding Visual
    Perception" and investigated in "On Using SIFT Descriptors for Image Parameter Evaluation"

    """

    def __init__(self, patch_size: PatchSize):
        """
        Constructor

        :param patch_size: Tthe patch size to calculate the parameter over.
        :type patch_size: :py:class:`swdatatoolkit.imageparam.PatchSize`
        """
        super().__init__(patch_size)

    @staticmethod
    def _calc_func(data: np.ndarray) -> np.ndarray:
        kurt_val = moment(data, moment=4, axis=None)
        if kurt_val == 0:
            return 0.0
        std_val = np.std(data)

        # TContrast = (sd ^ 2)/(kurtosis ^ 0.25)
        val = np.power(std_val, 2) / np.power(kurt_val, 0.25)
        if np.isnan(val):
            return 0.0

        return val

    @property
    def calc_func(self) -> Callable:
        return self._calc_func



import math
import numpy
import numpy as np


class Gradient:
    """
    This class is for holding the gradient values of an image in the x and y
    direction.
    """

    def __init__(self, gx: numpy.ndarray = None, gy: numpy.ndarray = None, gd: numpy.ndarray = None, nx: int = None,
                 ny: int = None):
        """
        Constructor requires nx and ny to be set if not passing in gx. Otherwise, it assumes that gy and gd are provided
        and the same shape of gx or need to be constructed in the same shape as gx.

        :param gx: A 2D matrix representing some image
        :param gy: A 2D matrix representing some image
        :param gd: A 2D matrix representing some image
        :param nx: The number of columns in the image this gradient object represents
        :param ny: The number of rows in the image ths gradient object represents
        :type gx: :py:class:`numpy.ndarray`
        :type gy: :py:class:`numpy.ndarray`
        :type gd: :py:class:`numpy.ndarray`
        :type nx: int
        :type ny: int
        :raises ValueError: If no `nx` and `ny` are supplied when no `gx` array is supplied.  Also if `gy` or `gd` are
                    supplied and are of a different size than a supplied `gx` or the `nx` and `ny` size.

        """
        if gx is None:
            if nx is None or ny is None:
                raise ValueError("nx and ny cannot be none when gx is not set!")
            else:
                self._gx = numpy.zeros([ny, nx])
        else:
            self._gx = gx
            ny = gx.shape[0]
            nx = gx.shape[1]

        if gy is None:
            self._gy = numpy.zeros([ny, nx])
        else:
            if gy.shape[0] == ny and gy.shape[1] == nx:
                self._gy = gy
            else:
                raise ValueError("gy array shape must match supplied size of gx")

        if gd is None:
            self._gd = numpy.zeros([ny, nx])
        else:
            if gd.shape[0] == ny and gd.shape[1] == nx:
                self._gd = gd
            else:
                raise ValueError("gd array shape must match supplied size of gx")

    @property
    def gx(self) -> numpy.ndarray:
        """
        In case of Gradient in the Cartesian system, this is the matrix of gradient values when comparing pixels
        in the X direction. In case of Gradient in the Polar system, this is the matrix of angles.

        :return:
        """
        return self._gx

    @gx.setter
    def gx(self, x, y, value):
        self._gx[y, x] = value

    @property
    def gy(self) -> numpy.ndarray:
        """
        In case of Gradient in the Cartesian system, this is the matrix of gradient values when comparing pixels
        in the Y direction. In case of Gradient in the Polar system, this is the matrix of Radii.

        :return:
        """
        return self._gy

    @gy.setter
    def gy(self, x, y, value):
        self._gy[y, x] = value

    @property
    def gd(self) -> numpy.ndarray:
        """
        This is the same for both Cartesian and Polar system. This is an auxiliary matrix to help distinguish
        the zero values in the Polar system. Gradient in the Polar system gets zero in the following cases and
        without 'gd' there is no way to distinguish them: 1. gx[i][j] = 0, gy[i][j] = 0 --&gt; because (i,j)
        lies on a solid area. 2. gx[i][j] = 0, gy[i][j] = 0 --&gt; because (i,j) lies on a vertical line of
        width 1 px. 3. gx[i][j] = 0, gy[i][j] = 0 --&gt; because (i,j) lies on a horizontal line of width 2 px.

        :return:
        """
        return self._gd

    @gd.setter
    def gd(self, x, y, value):
        self._gd[y, x] = value


class GradientCalculator:
    """
    A class for calculating the gradient of pixel intensities on source images.
    This class uses different kernels, depending on the provided gradient operator name, and calculated both horizontal
    and vertical derivatives, as well as the magnitude and angle of changes in the color intensity of pixels.

    There exists other operators which have not been yet implemented in this class:
        - `Prewitt operator <https://en.wikipedia.org/wiki/Prewitt_operator>`_
        - `Sobel operator <https://en.wikipedia.org/wiki/Sobel_operator>`_
        - `Roberts operator <https://en.wikipedia.org/wiki/Roberts_cross>`_

    """

    def __init__(self, gradient_name: str = 'sobel'):
        """
        This constructor sets the gradient operator based on the given name.

        :param gradient_name: Name of the gradient operator among the following options:

            - prewitt: `Prewitt operator <https://en.wikipedia.org/wiki/Prewitt_operator>`_
            - sobel: `Sobel operator <https://en.wikipedia.org/wiki/Sobel_operator>`_ (default)
            - roberts: `Roberts operator <https://en.wikipedia.org/wiki/Roberts_cross>`_
        :type gradient_name: str
        :raises NotImplementedError: If supplied string is something other than the above listed options

        """
        if gradient_name == 'prewitt':
            self._x_kernel = np.array([[-1, 0, 1], [-1, 0, 1], [-1, 0, 1]])
            self._y_kernel = np.array([[1, 1, 1], [0, 0, 0], [-1, -1, -1]])
        elif gradient_name == 'sobel':
            self._x_kernel = np.array([[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]])
            self._y_kernel = np.array([[-1, -2, -1], [0, 0, 0], [1, 2, 1]])
        elif gradient_name == 'roberts':
            self._x_kernel = np.array([[1, 0], [0, -1]])
            self._y_kernel = np.array([[0, 1], [-1, 0]])
        else:
            raise NotImplementedError("Filter Type {} not implemented.".format(gradient_name))

    def calculate_gradient_polar(self, image: numpy.ndarray) -> Gradient:
        """
        Calculates the gradient of pixel intensities on the input image and returns the results in a Polar coordinate
        system.

        :param image: A :py:class:`numpy.ndarray` 2D matrix representing some image
        :return: An object that represents the gradient as gx=theta and gy=r with range of theta is (-3.14, +3.14)
        :type image: :py:class:`numpy.ndarray`
        :rtype: :py:class:`swdatatoolkit.edgedetection.Gradient`
        :raises TypeError: If image is not an :py:class:`numpy.ndarray`
        :raises ValueError: If image is zero sized in either dimension

        """
        grad_cart = self.calculate_gradient_cart(image)
        gx = grad_cart.gx
        gy = grad_cart.gy
        gd = grad_cart.gd

        im_shape = image.shape

        # Angles of the gradient
        t_grad = np.zeros(im_shape)
        # Radius (magnitude) of the gradient
        r_grad = np.zeros(im_shape)

        for row in range(im_shape[0]):
            for col in range(im_shape[1]):
                r_grad[row, col] = math.hypot(gx[row, col], gy[row, col])
                """
                As the definition of atan2 indicates (Wikipedia/atan2), this function returns zero in 2 cases (this was 
                also tested and it confirms that there are ONLY these two cases:
                
                1. If x > 0, y = 0: This is a meaningful zero, since in a triangle, the angle against an edge of length 
                zero (y = 0) is zero. 
                
                2. If x = y = 0: This is mathematically undefined, but in Math library, atan2(0,0) returns zero. This
                zero represent a solid region with no particular texture. This zero should be treated differently.
                
                If we do not distinguish these two cases, then in the histogram of angles, we will not be able to ignore 
                the bin at hist[0] which is disproportionately larger than other bins.
                
                NOTE: In TDirectionalityParamCalculator, we set will hist[0] to zero to avoid detecting this bin as a 
                dominant peak.
                """

                # In case x=y=0, the followings statements are needed to distinguish horizontal (or vertical) lines on
                # a solid bg.

                if gx[row, col] == 0 and gy[row, col] == 0:
                    # If this pixel lies on a solid region
                    if gd[row, col] == 0:
                        t_grad[row, col] = - math.pi  # Reserving this constant for meaningless zeros on a solid region
                    elif gd[row, col] > 0:
                        # If this pixel lies on a vertical line
                        t_grad[row, col] = math.pi
                    else:
                        # If this pixel lies on a horizontal line
                        t_grad[row, col] = math.pi / 2.0
                elif gx[row, col] > 0 and gy[row, col] == 0:
                    t_grad[row, col] = math.pi  # This constant is reserved for meaningful zeros
                else:
                    t_grad[row, col] = math.atan2(gy[row, col], gx[row, col])

        polar_grad = Gradient(t_grad, r_grad, gd)
        return polar_grad

    def calculate_gradient_cart(self, image: numpy.ndarray) -> Gradient:
        """
        Calculates the gradient of pixel intensities on the input image and returns the results in the original
        Cartesian coordinate system.

        :param image: A 2D matrix representing some image
        :return: An object that represents the gradient in x and y direction
        :type image: :py:class:`numpy.ndarray`
        :rtype: :py:class:`swdatatoolkit.edgedetection.Gradient`
        :raises: :TypeError: If image is not an :py:class:`numpy.ndarray`
        :raises: :ValueError: If image is zero sized in either dimension
        """
        if not isinstance(image, numpy.ndarray):
            raise TypeError('The image must be of type numpy.ndarray!')

        im_shape = image.shape
        if im_shape[0] == 0 or im_shape[1] == 0:
            raise ValueError('The image must be non-zero size!')

        x_grad = np.zeros(im_shape)
        y_grad = np.zeros(im_shape)
        d_grad = np.zeros(im_shape)

        grad = Gradient(x_grad, y_grad, d_grad)

        kern_shape = self._x_kernel.shape
        for row in range(im_shape[0]):
            for col in range(im_shape[1]):

                if not (row == 0 or row == im_shape[0] - 1 or col == 0 or col == im_shape[1] - 1):
                    # xsum = 0.0
                    # ysum = 0.0
                    im_slice = image[row - 1:row + kern_shape[0] - 1, col - 1:col + kern_shape[1] - 1]

                    xsum = numpy.tensordot(im_slice, self._x_kernel, axes=2)
                    ysum = numpy.tensordot(im_slice, self._y_kernel, axes=2)
                    # for kr in range(kern_shape[0]):
                    #    for kc in range(kern_shape[1]):
                    #        px = im_slice[kr, kc]
                    #        xsum += (px * self._x_kernel[kr, kc])
                    #        ysum += px * self._y_kernel[kr, kc]
                    print(row,col)
                    x_grad[row, col] = float(xsum)
                    y_grad[row, col] = float(ysum)
                    d_grad[row, col] = abs(im_slice[0, 1] - im_slice[1, 0])

        return grad