use geozero::ToGeo;
use flatgeobuf::{FgbReader, FallibleStreamingIterator, reader_state::FeaturesSelectedSeek, FgbFeature, FeatureProperties, GeometryType as FgbGeometryType};
use geo::{Geometry, Point};
use regex::Regex;

use gdal::{
	Dataset, Metadata,
// The `LayerAccess` trait enables reading of vector specific fields from the `Dataset`.
	vector::{LayerAccess, OwnedFeatureIterator, Feature as GdalFeature, FieldValue},
};

use std::{
	io::{BufReader, BufWriter, A, Seek},
	fs::{File, remove_file},
	marker::PhantomData,
	error::Error,
	sync::Arc,
};


trait FormatDriver<'a, A, F>
	where A: AutoStruct<'a, F>,
	F: FeatureReader<'a> + Iterator<Item = Result<A, Box<dyn Error>>> {
	fn can_open(path: &str) -> bool;
	fn from_path(path: &'a str) -> Result<Self, Box<dyn Error>>
		where Self: Sized;
	//fn get_field_val<T>(&self, name: &str) -> T;
	//fn next_feature<T>(&mut self) -> Option<Result<T, Box<dyn Error>>>;
	type FeatureReader: FeatureReader<'a> + Iterator<Item = Result<A, Box<dyn Error>>>;
	fn iter(&'a mut self) -> Result<Self::FeatureReader, Box<dyn Error>>;
}

trait FeatureReader<'a> {
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>>; // Ok(false) -> end loop
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>>;
	fn get_field_point(&self, field_name: &str) -> Result<Option<Point>, Box<dyn Error>>;
}

trait AutoStruct<'a, F>
where F: FeatureReader<'a> {
	fn create(reader: &F) -> Self;
}

struct GpkgDriver<'a, T> {
	fi: OwnedFeatureIterator,
	p: PhantomData<&'a T>

}

const PATH_REGEXP:&str = r"^(?P<file_path>(?:.*/)?(?P<file_name>(?:.*/)?(?P<file_own_name>.*)\.(?P<extension>gpkg)))(?::(?P<layer_name>[a-z0-9_-]+))?$";

impl<'a, T, F> FormatDriver<'a, A, F> for GpkgDriver<'a, T> {
	type FeatureReader = GpkgLayer<'a>;
	fn can_open(path: &str) -> bool {
		let re = Regex::new(PATH_REGEXP).unwrap(); // the regexp is fixed, so it should crash only in tests
		re.is_match(&path)
	}

	fn from_path(path: &'a str) -> Result<Self, Box<dyn Error>> {
		let dataset = Dataset::open(path)?;
		let layer = dataset.into_layer(0)?; // TODO: choose layer from path expression or return error if can't choose
		let fi = layer.owned_features();
		Ok(Self { fi, p: PhantomData })
	}

	fn iter(&'a mut self) -> Result<GpkgLayer<'a>, Box<dyn Error>> {
		let fii = self.fi.into_iter();
		Ok(GpkgLayer { fii, fields: vec![], feature: None })
	}
}

struct GpkgLayer<'a> {
	fii: &'a mut OwnedFeatureIterator,
	fields: Vec<String>,
	feature: Option<GdalFeature<'a>>
}

impl<'a, A> Iterator for GpkgLayer<'a>
where A: AutoStruct<'a, F> {
	type Item = Result<A, Box<dyn Error>>;
	fn next(&self) -> Self::Item {
		if self.next_feature().is_some() {
			Some(A::create(&self))
		} else { None }
	}
}

impl<'a> FeatureReader<'a> for GpkgLayer<'a> {
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>> {
		if let Some(f) = self.fii.next() {
			self.feature.replace(f);
			Ok(true)
		}
		else { Ok(false) }
	}
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>> {
		match match match self.feature {
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

	fn get_field_point(&self, field_name: &str) -> Result<Option<Point>, Box<dyn Error>> {
		match match self.feature {
			Some(f) => f.geometry().to_geo(),
			None => panic!("no feature read yet")
		} {
			Ok(Geometry::Point(g)) => Ok(Some(g))
		}
	}
}

impl<'a, T> GpkgDriver<'a, T> {
	fn iter_features<U>(&mut self) -> &Self {
		// self // тут надо прочитать поля структуры, которую подают, и сделать какие-нибудь коллбэки?
		self
	}

	fn next_feature<U>(&mut self) -> Option<Result<T, Box<dyn Error>>> {
		todo!()
	}
}


struct FgbDriver<'a> {
	fp: File,
	features: Option<FgbReader<'a, File, FeaturesSelectedSeek>>
}

impl<'a, F> FormatDriver<'a, A, F> for FgbDriver<'a> {
	type FeatureReader = FgbFeatureReader<'a>;
	fn can_open(path: &str) -> bool {
		path.ends_with(".fgb")
	}

	fn from_path(path: &str) -> Result<Self, Box<dyn Error>> {
		let fp = File::open(path)?;
		Ok(Self { fp, features: None })
	}

	fn iter(&'a mut self) -> Result<Self::FeatureReader, Box<dyn Error>> {
		let features_selected = FgbReader::open(&mut self.fp)?.select_all()?;
		Ok(Self::FeatureReader { features_selected })
	}
}

struct FgbFeatureReader<'a> {
	features_selected: FgbReader<'a, File, FeaturesSelectedSeek>,
}

impl<'a> FeatureReader<'a> for FgbFeatureReader<'a> {
	fn next_feature(&mut self) -> Result<bool, Box<dyn Error>> {
		// getters should use self.features_selected.get() to get current feature
		Ok(self.features_selected.next()?.is_some())
	}
	fn get_field_i32(&self, field_name: &str) -> Result<Option<i32>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		Ok(Some(ft.property::<i32>(field_name)?))
	}
	fn get_field_point(&self, field_name: &str) -> Result<Option<Point>, Box<dyn Error>> {
		let ft = self.features_selected.cur_feature();
		match ft.to_geo()? {
			Geometry::Point(p) => Ok(Some(p)),
			_ => panic!("wrong geometry type!")
		}
	}
}

struct MyStruct {
	id: i32,
	geometry: Point
}

impl<'a, F> AutoStruct<'a, F> for MyStruct
where F: FeatureReader<'a> {
	fn create(reader: &F) -> Result<Self, Box<dyn Error>> {
		Ok(Self {
			id: reader.get_field_i32(0)?,
			geometry: reader.get_field_point(1)?
		})
	}
}


fn main() -> Result<(), Box<dyn Error>> {
	let p = vec![
		"places.gpkg:cities",
		"places.gpkg",
		"places",
		"saontehusa.gpkg",
		"sanhutens.gpkg:snoahtu:gosat"
	];
	for i in p.iter() {
		if GpkgDriver::<bool>::can_open(i) { println!("can open {}", i); }
		else { println!("can't open {}", i); }
	}

	let mut fd = FgbDriver::from_path("local.fgb")?;
	let fdi = fd.iter()?;

	while fdi.next_feature()? {

	}

	Ok(())
}
