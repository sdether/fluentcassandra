﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	internal static class ObjectHelper
	{
		public static List<byte[]> ToByteArrayList(this List<BytesType> list)
		{
			return list.Select(x => (byte[])x).ToList();
		}

		public static Clock ToClock(this DateTimeOffset dt)
		{
			return new Clock { Timestamp = DateTimeOffset.Now.UtcTicks };
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static IFluentBaseColumn<CompareWith> ConvertToFluentBaseColumn<CompareWith, CompareSubcolumnWith>(ColumnOrSuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			if (col.Super_column != null)
				return ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(col.Super_column);
			else if (col.Column != null)
				return ConvertColumnToFluentColumn<CompareWith>(col.Column);
			else
				return null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static FluentColumn<CompareWith> ConvertColumnToFluentColumn<CompareWith>(Column col)
			where CompareWith : CassandraType
		{
			return new FluentColumn<CompareWith> {
				ColumnName = CassandraType.GetType<CompareWith>(col.Name),
				ColumnValue = col.Value,
				ColumnTimestamp = new DateTimeOffset(col.Clock.Timestamp, TimeSpan.Zero),
				ColumnTimeToLive = col.Ttl
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="col"></param>
		/// <returns></returns>
		public static FluentSuperColumn<CompareWith, CompareSubcolumnWith> ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(SuperColumn col)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superCol = new FluentSuperColumn<CompareWith, CompareSubcolumnWith> {
				ColumnName = CassandraType.GetType<CompareWith>(col.Name)
			};

			foreach (var xcol in col.Columns)
				superCol.Columns.Add(ConvertColumnToFluentColumn<CompareSubcolumnWith>(xcol));

			return superCol;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
		public static Mutation CreateDeletedColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			var columnNames = mutation.Select(m => m.Column.ColumnName).ToList();

			var deletion = new Deletion {
				Clock = DateTimeOffset.Now.ToClock(),
				Predicate = CreateSlicePredicate(columnNames)
			};

			return new Mutation {
				Deletion = deletion
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
		public static Mutation CreateDeletedSuperColumnMutation(IEnumerable<FluentMutation> mutation)
		{
			var superColumn = mutation.Select(m => m.Column.GetPath().SuperColumn.ColumnName).FirstOrDefault();
			var columnNames = mutation.Select(m => m.Column.ColumnName).ToList();

			var deletion = new Deletion {
				Clock = DateTimeOffset.Now.ToClock(),
				Super_column = superColumn,
				Predicate = CreateSlicePredicate(columnNames)
			};

			return new Mutation {
				Deletion = deletion
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutation"></param>
		/// <returns></returns>
		public static Mutation CreateInsertedOrChangedMutation(FluentMutation mutation)
		{
			switch (mutation.Type)
			{
				case MutationType.Added:
				case MutationType.Changed:
					return new Mutation {
						Column_or_supercolumn = CreateColumnOrSuperColumn(mutation.Column)
					};

				default:
					return null;
			}
		}

		public static Column CreateColumn(IFluentColumn column)
		{
			return new Column {
				Name = column.ColumnName,
				Value = column.ColumnValue,
				Clock = column.ColumnTimestamp.ToClock()
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="column"></param>
		/// <returns></returns>
		public static ColumnOrSuperColumn CreateColumnOrSuperColumn(IFluentBaseColumn column)
		{
			if (column is IFluentColumn)
			{
				return new ColumnOrSuperColumn {
					Column = CreateColumn((IFluentColumn)column)
				};
			}
			else if (column is IFluentSuperColumn)
			{
				var colY = (IFluentSuperColumn)column;
				var superColumn = new SuperColumn {
					Name = colY.ColumnName,
					Columns = new List<Column>()
				};

				foreach (var col in colY.Columns.OfType<IFluentColumn>())
					superColumn.Columns.Add(CreateColumn(col));

				return new ColumnOrSuperColumn {
					Super_column = superColumn
				};
			}
			else
			{
				return null;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columnNames"></param>
		/// <returns></returns>
		public static SlicePredicate CreateSlicePredicate(List<CassandraType> columnNames)
		{
			return new SlicePredicate {
				Column_names = columnNames.Cast<byte[]>().ToList()
			};
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="start"></param>
		/// <param name="finish"></param>
		/// <param name="reversed"></param>
		/// <param name="count"></param>
		/// <returns></returns>
		public static SlicePredicate CreateSlicePredicate(byte[] start, byte[] finish, bool reversed = false, int count = 100)
		{
			return new SlicePredicate {
				Slice_range = new SliceRange {
					Start = start,
					Finish = finish,
					Reversed = reversed,
					Count = count
				}
			};
		}
	}
}
