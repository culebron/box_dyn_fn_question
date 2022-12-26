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

trait FormatDriver<'a, A>
where A: AutoStruct {
	fn can_open(path: &str) -> bool;
	fn from_path(path: &str) -> Result<Self, Box<dyn Error>>
		where Self: Sized;
	// create a reader (ideally this should look like for loop, but not right now)
	type FeatureReader: FeatureReader + Iterator<Item=A>;
	fn iter(&'a mut self) -> Result<Self::FeatureReader, Box<dyn Error>>;
}

trait FeatureReader {
	// forward the reader 1 record
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>>; // Ok(false) -> end loop
	// accessors sort of like in Serde
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>>;
	fn get_field_point(&self, field_name: &str) -> Result<Option<Point>, Box<dyn Error>>;
}


// FORMAT DRIVER 1: GPKG (via GDAL)
struct GpkgDriver<'a, A> {
	fi: OwnedFeatureIterator,
	_p: PhantomData<&'a A>
}

const PATH_REGEXP:&str = r"^(?P<file_path>(?:.*/)?(?P<file_name>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg)))(?::(?P<layer_name>[a-z0-9_-]+))?$";

impl<'a, A: AutoStruct> FormatDriver<'a, A> for GpkgDriver<'a, A> {
	type FeatureReader = GpkgLayer<'a, A>;
	fn can_open(path: &str) -> bool {
		let re = Regex::new(PATH_REGEXP).unwrap();
		re.is_match(&path)
	}

	fn from_path(path: &str) -> Result<Self, Box<dyn Error>> {
		let dataset = Dataset::open(path)?;
		// TODO: choose layer from path expression or return error if can't choose
		let layer = dataset.into_layer(0)?;
		let fi = layer.owned_features();
		Ok(Self { fi, _p: PhantomData })
	}

	fn iter(&'a mut self) -> Result<GpkgLayer<'a, A>, Box<dyn Error>> {
		let fii = self.fi.into_iter();
		Ok(GpkgLayer { fii, fields: vec![], feature: None, _p: PhantomData })
	}
}

struct GpkgLayer<'a, A> {
	fii: &'a mut OwnedFeatureIterator,
	fields: Vec<String>,
	feature: Option<GdalFeature<'a>>,
	_p: PhantomData<&'a A>
}

impl<'a, A> FeatureReader for GpkgLayer<'a, A> {
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>> {
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

impl<'a, A> Iterator for GpkgLayer<'a, A>
where A: AutoStruct{
	type Item = A;
	fn next(&mut self) -> Option<Self::Item> {
		todo!()
	}
}
// FORMAT DRIVER 2: FGB (FlatGeobuf)
// this format wants &File as input,
// so I must either a) open the file outside, or b) have 2 structs
struct FgbDriver<'a, A> {
	fp: File,
	features: Option<FgbReader<'a, File, FeaturesSelectedSeek>>,
	_p: PhantomData<A>
}

impl<'a, A: AutoStruct> FormatDriver<'a, A> for FgbDriver<'a, A> {
	type FeatureReader = FgbFeatureReader<'a, A>;
	fn can_open(path: &str) -> bool {
		path.ends_with(".fgb")
	}

	fn from_path(path: &str) -> Result<Self, Box<dyn Error>> {
		let fp = File::open(path)?;
		Ok(Self { fp, features: None, _p: PhantomData })
	}

	fn iter(&'a mut self) -> Result<Self::FeatureReader, Box<dyn Error>> {
		let features_selected = FgbReader::open(&mut self.fp)?.select_all()?;
		Ok(Self::FeatureReader { features_selected, _p: PhantomData })
	}
}

struct FgbFeatureReader<'a, A> {
	features_selected: FgbReader<'a, File, FeaturesSelectedSeek>,
	_p: PhantomData<A>
}

impl<'a, A> FeatureReader for FgbFeatureReader<'a, A> {
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>> {
		// getters should use self.features_selected.get() to get current feature
		Ok(self.features_selected.next()?.is_some())
	}
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		Ok(Some(ft.property::<i32>(field_name)?))
	}
	fn get_field_point(&self, _field_name: &str) -> Result<Option<Point>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		match ft.to_geo()? {
			Geometry::Point(p) => Ok(Some(p)),
			_ => panic!("wrong geometry type!")
		}
	}
}

impl<'a, A> Iterator for FgbFeatureReader<'a, A>
where A: AutoStruct {
	type Item = A;
	fn next(&mut self) -> Option<Self::Item> {
		todo!()
	}
}

// this should have some code to work with the drivers, like `from_driver` below
trait AutoStruct {
	fn generate<F: FeatureReader>(reader: &F) -> Self;
}

struct MyStruct {
	id: i32,
	geometry: Point
}

impl AutoStruct for MyStruct {
	fn generate<F>(reader: &F) -> Self {
		todo!()
	}
}
impl MyStruct {
	fn get_fields() -> Vec<String> {
		vec!["id".to_string(), "geometry".to_string()]
	}

	// this is generic, but I should change this to using Box<dyn FeatureReader> inside here
	// because this is chosen at runtime
	fn from_driver<'a, D>(driver_iter: &'a D) -> Result<Self, Box<dyn Error>>
	where D: FeatureReader {

		Ok(Self {
			id: driver_iter.get_field_i32("num")?.unwrap(),
			geometry: driver_iter.get_field_point("geometry")?.unwrap()
		})
	}
}

// there'll be a function that will walk down the list of formats and check which one can open the file
// then call MyStruct::from_driver.


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
		if GpkgDriver::<'_, MyStruct>::can_open(i) { println!("Gpkg can open {:?}", i); }
		if FgbDriver::<'_, MyStruct>::can_open(i) { println!("Fgb can open {:?}", i); }
	}

	let mut fd: FgbDriver<MyStruct> = FgbDriver::from_path("local.fgb")?;
	//let fdi = fd.iter()?;

	//while fdi.next_feature()? {

	//}

	Ok(())
}


/*#[allow()]
use std::{marker::PhantomData, error::Error};

// this is the more general struct that tries opening the file
// some crates make a stack of borrowing structs, so we'll need at least 2 layers
// FormatDriver just opens it as far as borrowing allows
// FeatureReader will read feature data

trait FormatDriver<'a> {
	fn from_path(path: &'a str) -> Result<Self, Box<dyn Error>>
		where Self: Sized;
	type FeatureReader: FeatureReader + Iterator;
	fn iter(&'a mut self) -> Result<Self::FeatureReader, Box<dyn Error>>;
}

trait FeatureReader {
	// forward the reader 1 record
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>>; // Ok(false) -> end loop
	// accessors sort of like in Serde
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>>;
	fn get_field_point(&self, field_name: &str) -> Result<Option<Point>, Box<dyn Error>>;
}

fn main() {

}
*/
