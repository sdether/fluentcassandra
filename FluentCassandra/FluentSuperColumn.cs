﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Dynamic;
using System.ComponentModel;
using FluentCassandra.Types;

namespace FluentCassandra
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class FluentSuperColumn<CompareWith, CompareSubcolumnWith> : FluentRecord<IFluentColumn<CompareSubcolumnWith>>, IFluentSuperColumn<CompareWith, CompareSubcolumnWith>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		private FluentColumnList<IFluentColumn<CompareSubcolumnWith>> _columns;

		/// <summary>
		/// 
		/// </summary>
		public FluentSuperColumn()
		{
			_columns = new FluentColumnList<IFluentColumn<CompareSubcolumnWith>>(GetPath());
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="columns"></param>
		internal FluentSuperColumn(IEnumerable<IFluentColumn<CompareSubcolumnWith>> columns)
		{
			_columns = new FluentColumnList<IFluentColumn<CompareSubcolumnWith>>(GetPath(), columns);
		}

		/// <summary>
		/// The column name.
		/// </summary>
		public CompareWith Name { get; set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IFluentColumn<CompareSubcolumnWith> CreateColumn()
		{
			return new FluentColumn<CompareSubcolumnWith>();
		}

		/// <summary>
		/// The columns in the super column.
		/// </summary>
		public override IList<IFluentColumn<CompareSubcolumnWith>> Columns
		{
			get { return _columns; }
		}

		/// <summary>
		/// 
		/// </summary>
		public IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> Family
		{
			get;
			internal set;
		}

		/// <summary>
		/// Gets the path.
		/// </summary>
		/// <returns></returns>
		public FluentColumnPath GetPath()
		{
			return new FluentColumnPath(Family, this, null);
		}

		/// <summary>
		/// Gets the parent.
		/// </summary>
		/// <returns></returns>
		public FluentColumnParent GetParent()
		{
			return new FluentColumnParent(Family, null);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		public override bool TryGetColumn(object name, out object result)
		{
		    var col = Columns.FirstOrDefault(c => c.Name == name);

			result = (col == null) ? null : col.Value;
		    return col != null;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <returns></returns>
		public override bool TrySetColumn(object name, object value)
		{
			var col = Columns.FirstOrDefault(c => c.Name == name);
			var mutationType = MutationType.Changed;

			// if column doesn't exisit create it and add it to the columns
			if (col == null)
			{
				mutationType = MutationType.Added;

				col = new FluentColumn<CompareSubcolumnWith>();
				((FluentColumn<CompareSubcolumnWith>)col).Name = CassandraType.GetType<CompareSubcolumnWith>(name);

				_columns.SupressChangeNotification = true;
				_columns.Add(col);
				_columns.SupressChangeNotification = false;
			}

			// set the column value
			col.Value = CassandraType.GetType<BytesType>(value);

			// notify the tracker that the column has changed
			OnColumnMutated(mutationType, col);

			return true;
		}

		#region IFluentSuperColumn Members

		IEnumerable<IFluentColumn> IFluentSuperColumn.Columns { get { return _columns.OfType<IFluentColumn>(); } }

		#endregion

		#region IFluentBaseColumn Members

		CassandraType IFluentBaseColumn.Name { get { return Name; } }

		IFluentBaseColumnFamily IFluentBaseColumn.Family { get { return Family; } }

		void IFluentBaseColumn.SetParent(FluentColumnParent parent)
		{
			UpdateParent(parent);
		}

		private void UpdateParent(FluentColumnParent parent)
		{
			Family = parent.ColumnFamily as IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>;

			var columnParent = GetPath();
			_columns.Parent = columnParent;

			foreach (var col in Columns)
				col.SetParent(columnParent);

			ResetMutationAndAddAllColumns();
		}

		#endregion
	}
}