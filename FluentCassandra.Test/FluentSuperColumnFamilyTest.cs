﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test
{
	[TestClass]
	public class FluentSuperColumnFamilyTest
	{
		[TestMethod]
		public void Self_Set()
		{
			// arrange

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");

			// assert
			Assert.AreSame(actual, actual.GetSelf().ColumnFamily);
		}

		[TestMethod]
		public void Path_Set()
		{
			// arrange

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");

			// assert
			Assert.AreSame(actual, actual.GetPath().ColumnFamily);
		}

		[TestMethod]
		public void Constructor_Test()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test2" };

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			Assert.AreEqual(2, actual.Columns.Count);
			Assert.AreSame(col1.Family, actual);
			Assert.AreSame(col2.Family, actual);
		}

		[TestMethod]
		public void Constructor_Dynamic_Test()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test2" };

			// act
			dynamic actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = col1;
			actual.Test2 = col2;

			// assert
			Assert.AreEqual(col1, actual.Test1);
			Assert.AreEqual(col1, actual["Test1"]);
			Assert.AreEqual(col2, actual.Test2);
			Assert.AreEqual(col2, actual["Test2"]);
			Assert.AreSame(col1.Family, actual);
			Assert.AreSame(col2.Family, actual);
		}

		[TestMethod]
		public void Mutation()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test2" };

			// act
			var actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			var mutations = actual.MutationTracker.GetMutations();

			Assert.AreEqual(2, mutations.Count());
			Assert.AreEqual(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.Name == "Test1");
			var mut2 = mutations.FirstOrDefault(x => x.Column.Name == "Test2");

			Assert.AreSame(col1, mut1.Column);
			Assert.AreSame(col2, mut2.Column);

			Assert.AreSame(actual, mut1.Column.GetParent().ColumnFamily);
			Assert.AreSame(actual, mut2.Column.GetParent().ColumnFamily);
		}

		[TestMethod]
		public void Dynamic_Mutation()
		{
			// arrange
			var col1 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test1" };
			var col2 = new FluentSuperColumn<AsciiType, AsciiType> { Name = "Test2" };

			// act
			dynamic actual = new FluentSuperColumnFamily<AsciiType, AsciiType>("Keyspace1", "Standard1");
			actual.Test1 = col1;
			actual.Test2 = col2;

			// assert
			var mutations = ((IFluentRecord)actual).MutationTracker.GetMutations();

			Assert.AreEqual(2, mutations.Count());
			Assert.AreEqual(2, mutations.Count(x => x.Type == MutationType.Added));

			var mut1 = mutations.FirstOrDefault(x => x.Column.Name == "Test1");
			var mut2 = mutations.FirstOrDefault(x => x.Column.Name == "Test2");

			Assert.IsNotNull(mut1);
			Assert.IsNotNull(mut2);

			Assert.AreSame(actual, mut1.Column.GetParent().ColumnFamily);
			Assert.AreSame(actual, mut2.Column.GetParent().ColumnFamily);
		}
	}
}