#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Handling of TESS data quality flags.

Source: https://github.com/tasoc/starclass
"""

import numpy as np

#--------------------------------------------------------------------------------------------------
class QualityFlagsBase(object):

	# Using this bitmask only QUALITY == 0 cadences will remain
	HARDEST_BITMASK = 2**32-1

	@classmethod
	def decode(cls, quality):
		"""
		Converts a QUALITY value into a list of human-readable strings.
		This function takes the QUALITY bitstring that can be found for each
		cadence in TESS data files and converts into a list of human-readable
		strings explaining the flags raised (if any).

		Parameters:
			quality (int): Value from the 'QUALITY' column of a TESS data file.

		Returns:
			list of str: List of human-readable strings giving a short
				description of the quality flags raised.
				Returns an empty list if no flags raised.
		"""
		result = []
		for flag in cls.STRINGS.keys():
			if quality & flag != 0:
				result.append(cls.STRINGS[flag])
		return result

	@classmethod # noqa: A003
	def filter(cls, quality, flags=None):
		"""
		Filter quality flags against a specific set of flags.

		Parameters:
			quality (integer or ndarray): Quality flags.
			flags (integer bitmask): Default=``TESSQualityFlags.DEFAULT_BITMASK``.

		Returns:
			ndarray: ``True`` if quality DOES NOT contain any of the specified ``flags``, ``False`` otherwise.

		"""
		if flags is None:
			flags = cls.DEFAULT_BITMASK
		return (quality & flags == 0)

	@staticmethod
	def binary_repr(quality):
		"""
		Binary representation of the quality flag.

		Parameters:
			quality (int or ndarray): Quality flag.

		Returns:
			string: Binary representation of quality flag. String will be 32 characters long.

		"""
		if isinstance(quality, (np.ndarray, list, tuple)):
			return np.array([np.binary_repr(q, width=32) for q in quality])
		else:
			return np.binary_repr(quality, width=32)


#--------------------------------------------------------------------------------------------------
class TESSQualityFlags(QualityFlagsBase):
	"""
	This class encodes the meaning of the various TESS PIXEL_QUALITY bitmask flags.
	"""
	AttitudeTweak = 1 #: Attitude tweak
	SafeMode = 2 #: Safe mode
	CoarsePoint = 4 #: Spacecraft in Coarse point
	EarthPoint = 8 #: Spacecraft in Earth point
	ZeroCrossing = 16 #: Reaction wheel zero crossing
	Desat = 32 #: Reaction wheel desaturation event
	ApertureCosmic = 64 #: Cosmic ray in optimal aperture pixel
	ManualExclude = 128 #: Manual exclude
	SensitivityDropout = 256 #: Sudden sensitivity dropout
	ImpulsiveOutlier = 512 #: Impulsive outlier
	CollateralCosmic = 1024 #: Cosmic ray in collateral data
	EarthMoonPlanetInFOV = 2048 #: Earth, Moon or other planet in camera FOV
	ScatteredLight = 4096 #: Scattered light from Earth or Moon in CCD

	# Which is the recommended QUALITY mask to identify bad data?
	DEFAULT_BITMASK = (AttitudeTweak | SafeMode | CoarsePoint | EarthPoint | Desat
		| ApertureCosmic | ManualExclude | ScatteredLight)

	# Preferred bitmask for CBV corrections
	CBV_BITMASK = (SafeMode | EarthPoint | Desat | ManualExclude)

	# This bitmask includes flags that are known to identify both good and bad cadences.
	# Use it wisely.
	HARD_BITMASK = (DEFAULT_BITMASK | SensitivityDropout | CollateralCosmic)

	# Pretty string descriptions for each flag
	STRINGS = {
		AttitudeTweak: "Attitude tweak",
		SafeMode: "Safe mode",
		CoarsePoint: "Spacecraft in Coarse point",
		EarthPoint: "Spacecraft in Earth point",
		ZeroCrossing: "Reaction wheel zero crossing",
		Desat: "Reaction wheel desaturation event",
		ApertureCosmic: "Cosmic ray in optimal aperture pixel",
		ManualExclude: "Manual exclude",
		SensitivityDropout: "Sudden sensitivity dropout",
		ImpulsiveOutlier: "Impulsive outlier",
		CollateralCosmic: "Cosmic ray in collateral data",
		EarthMoonPlanetInFOV: "Earth, Moon or other planet in camera FOV",
		ScatteredLight: "Scattered light from Earth or Moon in CCD"
	}