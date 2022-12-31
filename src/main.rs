use std::{fs::{File}, marker::PhantomData, error::Error};
use regex::Regex;

use geo::{Geometry, Point};
use geozero::ToGeo;
use flatgeobuf::{FgbReader, FallibleStreamingIterator, reader_state::FeaturesSelectedSeek, FeatureProperties};
use gdal::{Dataset, vector::{OwnedFeatureIterator, Feature as GdalFeature, FieldValue}};

// this is the more general struct that tries opening the file
// some crates make a stack of borrowing structs, so we'll need at least 2 layers
// FormatDriver just opens it as far as borrowing allows
// FeatureReader will read feature data

trait FormatDriver {
	fn can_open(path: &str) -> bool where Self: Sized;
	fn from_path(path: &str) -> Result<Self, Box<dyn Error>>
		where Self: Sized;
	// create a reader (ideally this should look like for loop, but not right now)
	type Layer: FeatureReader;
	fn iter(&mut self) -> Result<Self::Layer, Box<dyn Error>>;
}

trait FeatureReader {
	// forward the reader 1 record
	fn forward(&mut self) -> Result<bool, Box<dyn Error>>; // Ok(false) -> end loop
	// accessors sort of like in Serde
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>>;
	fn get_field_i64(&self, field_name: &str) -> Result<Option<i64>, Box<dyn Error>>;
	fn get_field_point(&self, field_name: &str) -> Result<Option<Point>, Box<dyn Error>>;
}


// this should have some code to work with the drivers, like `from_driver` below
trait AutoStruct<'a> {
	fn generate<F: FeatureReader>(reader: &F) -> Result<Self, Box<dyn Error>> where Self: Sized;
}

// FORMAT DRIVER 1: GPKG (via GDAL)
struct GpkgDriver<'a> {
	fi: OwnedFeatureIterator,
	p: PhantomData<&'a bool>
}

const PATH_REGEXP:&str = r"^(?P<file_path>(?:.*/)?(?P<file_name>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg)))(?::(?P<layer_name>[a-z0-9_-]+))?$";

impl<'a> FormatDriver for GpkgDriver<'a> {
	type Layer = GpkgLayer<'a>;
	fn can_open(path: &str) -> bool {
		let re = Regex::new(PATH_REGEXP).unwrap();
		re.is_match(&path)
	}

	fn from_path(path: &str) -> Result<Self, Box<dyn Error>> {
		let dataset = Dataset::open(path)?;
		// TODO: choose layer from path expression or return error if can't choose
		let layer = dataset.into_layer(0)?;
		let fi = layer.owned_features();
		Ok(Self { fi, p: PhantomData })
	}

	fn iter(&mut self) -> Result<Self::Layer, Box<dyn Error>> {
		let fii = self.fi.into_iter();
		Ok(GpkgLayer { fii, feature: None })
	}
}

struct GpkgLayer<'a> {
	fii: &'a mut OwnedFeatureIterator,
	feature: Option<GdalFeature<'a>>,
}

impl<'a> FeatureReader for GpkgLayer<'a> {
	fn forward(&mut self) -> Result<bool, Box<dyn Error>> {
		if let Some(f) = self.fii.next() {
			self.feature.replace(f);
			Ok(true)
		}
		else { Ok(false) }
	}
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>> {
		match match match &self.feature {
			Some(f) => f.field(field_name)?,
			None => panic!("no feature but reading field")
		} {
			Some(v) => v,
			None => return Ok(None),
		} {
			FieldValue::IntegerValue(v) => Ok(Some(v.into())),
			FieldValue::Integer64Value(v) => Ok(Some(v.try_into()?)),
			_ => panic!("wrong format")
		}
	}
	fn get_field_i64(&self, field_name: &str) -> Result<Option<i64>, Box<dyn Error>> {
		match match match &self.feature {
			Some(f) => f.field(field_name)?,
			None => panic!("no feature but reading field")
		} {
			Some(v) => v,
			None => return Ok(None),
		} {
			FieldValue::IntegerValue(v) => Ok(Some(v.into())),
			FieldValue::Integer64Value(v) => Ok(Some(v.try_into()?)),
			_ => panic!("wrong format")
		}
	}

	fn get_field_point(&self, _field_name: &str) -> Result<Option<Point>, Box<dyn Error>> {
		match match &self.feature {
			Some(f) => Some(f.geometry().to_geo()?),
			None => panic!("no feature read yet"),
			_ => None::<Geometry> // TODO: this is just to fix the non-exhaustive patterns
		} {
			Some(Geometry::Point(g)) => Ok(Some(g)),
			// just to fix the return types/exhaustiveness
			None => Ok(None),
			_ => panic!("what have I just got?")
		}
	}
}

// FORMAT DRIVER 2: FGB (FlatGeobuf)
// this format wants &File as input,
// so I must either a) open the file outside, or b) have 2 structs
struct FgbDriver<'a> {
	fp: File,
	p: PhantomData<&'a bool>
}

impl<'a> FormatDriver for FgbDriver<'a> {
	type Layer = FgbFeatureReader<'a>;
	fn can_open(path: &str) -> bool {
		path.ends_with(".fgb")
	}

	fn from_path(path: &str) -> Result<Self, Box<dyn Error>> {
		let fp = File::open(path)?;
		Ok(Self { fp, p: PhantomData })
	}

	fn iter(&mut self) -> Result<Self::Layer, Box<dyn Error>> {
		let features_selected = FgbReader::open(&mut self.fp)?.select_all()?;
		Ok(FgbFeatureReader { features_selected })
	}
}

struct FgbFeatureReader<'a> {
	features_selected: FgbReader<'a, File, FeaturesSelectedSeek>,
}

impl<'a> FeatureReader for FgbFeatureReader<'a> {
	fn forward(&mut self) -> Result<bool, Box<dyn Error>> {
		// getters should use self.features_selected.get() to get current feature
		Ok(self.features_selected.next()?.is_some())
	}
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		Ok(Some(ft.property::<i32>(field_name)?))
	}
	fn get_field_i64(&self, field_name: &str) -> Result<Option<i64>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		Ok(Some(ft.property::<i64>(field_name)?))
	}
	fn get_field_point(&self, _field_name: &str) -> Result<Option<Point>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		match ft.to_geo()? {
			Geometry::Point(p) => Ok(Some(p)),
			_ => panic!("wrong geometry type!")
		}
	}
}

struct BoxDriver<T>(T);

impl<'a, T: FormatDriver> FormatDriver for BoxDriver<T>
where T::Layer: 'static {
	type Layer = Box<dyn FeatureReader>;
	fn can_open(_: &str) -> bool { false }
	fn from_path(path: &str) -> Result<Self, Box<dyn Error>> { todo!() }
	fn iter(&mut self) -> Result<Self::Layer, Box<dyn Error>> { todo!() }

}

#[derive(Debug)]
struct MyStruct {
	x: i64,
	geometry: Point
}

impl<'a> AutoStruct<'a> for MyStruct {
	fn generate<F: FeatureReader>(reader: &F) -> Result<Self, Box<dyn Error>> {
		Ok(Self {
			x: reader.get_field_i64("x")?.unwrap(),
			geometry: reader.get_field_point("geometry")?.unwrap()
		})
	}
}

fn main() -> Result<(), Box<dyn Error>> {
	let p = vec![
		"places.gpkg:cities",
		"places.gpkg",
		"places",
		"saontehusa.gpkg",
		"sanhutens.gpkg:snoahtu:gosat",
		"asoneht.fgb",
		"aosnetuh"
	];
	for i in p.iter() {
		let rdr = if GpkgDriver::can_open(i) {
			println!("Gpkg can open {:?}", i);
			let mut x = GpkgDriver::from_path(i)?;
			Box::new(BoxDriver(x)) as Box<dyn FormatDriver<Layer = Box<dyn FeatureReader>>>
		}
		else if FgbDriver::<'_>::can_open(i) {
			println!("Fgb can open {:?}", i);
			Box::new(BoxDriver(FgbDriver::from_path(i)?)) as Box<dyn FormatDriver<Layer = Box<dyn FeatureReader>>>
		} else {
			println!("no format suits {}", i);
			continue;
		};
	}
	Ok(())
}
