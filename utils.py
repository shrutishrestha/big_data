"""
 swdatatoolkit, a project at the Data Mining Lab
 (http://dmlab.cs.gsu.edu/) of Georgia State University (http://www.gsu.edu/).

 Copyright (C) 2022 Georgia State University

 This program is free software: you can redistribute it and/or modify it under
 the terms of the GNU General Public License as published by the Free Software
 Foundation version 3.

 This program is distributed in the hope that it will be useful, but WITHOUT
 ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 details.

 You should have received a copy of the GNU General Public License along with
 this program. If not, see <http://www.gnu.org/licenses/>.
"""
from typing import List, Dict, Tuple

import numpy
import numpy as np

import numbers


class PeakDetector:
    """
    This class is a designed to find dominant peaks of a time series based on the settings provided by the user. For
    a peak to be considered dominant, three constraints can be set: a threshold on the frequency domain, a minimum
    peak-to-peak distance, and a maximum number of dominant peaks.

    The main task is done in the method :func:`~util.PeakDetection.find_peaks` which initially finds all the peaks
    (i.e., any data point whose value is larger than both of its previous and next neighbors), sorts them by their
    height, and then removes those which do not fall into the set constraints.

    """

    def __init__(self, width: int, threshold: float, max_peaks: int = 0, is_percentile: bool = True):
        """
        Constructor for the class PeakDetection

        :param width: The radius of the neighborhood of an accepted peak within which no other peaks is allowed. To
            relax this condition, set it to 1 or 0. The results would be similar, i.e., no peaks will be removed
            because of the neighboring constraints.
        :param threshold: On the frequency domain of the time series below which all the peaks will be ignored.
            To relax this condition, set it to the minimum frequency value of the given sequence. If used in conjunction
            with `is_percentile` values must be between 0 and 100.
        :param max_peaks: The maximum number of peaks to be found, from the highest to lowest. If `n=0`, all
            of the detected peaks will be returned.
        :param is_percentile: If `true`, then the value of `threshold` is interpreted as the percentile. Then the
            accepted values are doubles within [0,100]. If false, then the given `double` value will be used directly
            as the actual threshold on the frequency domain.
        :type width: int
        :type threshold: float
        :type max_peaks: int
        :type is_percentile: bool
        :return:

        """
        self._width = width
        self._threshold = threshold
        self._max_peaks = max_peaks
        self._is_percentile = is_percentile

        if self._is_percentile:
            if not isinstance(self._threshold, numbers.Number) or self._threshold > 100 or self._threshold < 0:
                raise ValueError("Percentile must lie within the interval [0, 100]!")

        if not isinstance(self._width, numbers.Number) or self._width < 0:
            raise ValueError("The values of `width` must be a numeric value and cannot be negative")

        if not isinstance(self._max_peaks, numbers.Number) or self._max_peaks < 0:
            raise ValueError("The values of `max_peaks` must be a numeric value and cannot be negative")

    def find_peaks(self, data: List[float]) -> List[int]:
        """
        This method finds the peaks of the given time series based on the constraints provided for the class

        The returned list is sorted by the height of the peaks, so the indices are not ordered.

        :param data: The time series that peaks are to be searched for in.
        :return: A list of index locations for the peaks meeting the criteria, sorted by height of the peak.
        """

        candidate_peaks = self._find_candidate_peaks(data)

        # Find the largest peaks and remove smaller peaks within the exclusion region of those peaks
        sorted_candidates = sorted(candidate_peaks, key=lambda item: item[1], reverse=True)

        removed_candidates = {}
        result_peaks = []
        for candidate_peak in sorted_candidates:
            if not candidate_peak[0] in removed_candidates:
                from_idx = candidate_peak[0] - self._width
                to_idx = candidate_peak[0] + self._width
                for idx in range(from_idx, to_idx + 1):
                    if idx in candidate_peaks and not idx == candidate_peak[0]:
                        val = candidate_peaks.pop(idx)
                        removed_candidates[idx] = val

                result_peaks.append(candidate_peak[0])

                # Check if limit has been reached
                if len(result_peaks) >= self._max_peaks:
                    break

        return result_peaks

    def _find_candidate_peaks(self, data: List[float]) -> List[Tuple[int, float]]:
        """
        Finds the position of any peaks on the time series, excluding those that are below the defined threshold.

        :param data: The time series that peaks are to be searched for in.
        :return: A list of tuples where peak positions within the original data that meet the defined threshold
            value is the first item and the height of the peak is the second.
        """
        mid = 1
        end = len(data)
        peaks = []

        threshold = 0.0
        if self._is_percentile:
            threshold = numpy.percentile(data, int(self._threshold))
        else:
            threshold = self._threshold

        # Adding points slightly more negative to the ends allows for the beginning and end to be possible peaks
        data_cpy = [data[0] - 0.1] + data
        data_cpy.append(data[-1] - 0.1)

        while mid <= end:
            if data_cpy[mid - 1] < data_cpy[mid] and data_cpy[mid + 1] < data_cpy[mid] \
                    and data[mid - 1] > threshold:
                peaks.append((mid - 1, data[mid - 1]))
            mid += 1
        return peaks
