use colegiosOficiales;

db.createCollection("ubicacion");
db.createCollection("tipoEstable");
db.createCollection("estable");
db.createCollection("correo");

db.ubicacion.createIndex({ idUbica: 1 }, { unique: true });
db.tipoEstable.createIndex({ idTipoEstable: 1 }, { unique: true });
db.estable.createIndex({ idEstable: 1 }, { unique: true });
db.correo.createIndex({ idEstable: 1 }, { unique: true });


db.dataBruto.aggregate([
    {
        $project: {
            _id: 0,
            zona: 1,
            direccion: 1,
            idUbica: "$_id"
        }
    },
    {
        $merge: {
            into: "ubicacion",
            on: "idUbica",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.dataBruto.aggregate([
    {
        $project: {
            _id: 0,
            tipo_Establecimiento: 1,
            especialidad: 1,
            jornadas: 1,
            idTipoEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "tipoEstable",
            on: "idTipoEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.dataBruto.aggregate([
    {
        $project: {
            _id: 0,
            nomEstable: "$nombreestablecimiento",
            numSedes: "$numero_de_Sedes",
            nomRector: "$nombre_Rector",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.dataBruto.aggregate([
    {
        $lookup: {
            from: "estable",
            localField: "idEstable",
            foreignField: "codMuni",
            as: "municipioInfo"
        }
    },
    {
        $project: {
            _id: 0,
            nomEstable: "$nombreestablecimiento",
            numSedes: "$numero_de_Sedes",
            nomRector: "$nombre_Rector",
            idEstable: "$_id"
        }
    },
    {
        $merge: {
            into: "estable",
            on: "idEstable",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

