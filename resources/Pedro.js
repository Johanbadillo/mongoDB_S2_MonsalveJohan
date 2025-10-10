use incautaciones_db2;

    /*
    - `incautaciones(fecha_hecho, cod_muni, cantidad, id_unidad)`
    - `municipios(cod_muni, nombre_muni, cod_depto)`
    - `departamentos(cod_depto, nombre_depto)`
    - `unidades(_id, nombre_unidad)`
    */
    //CUIDADO!!!! Eliminación de datos "crudos" o sin procesar
    db.incautaciones_raw.drop();
    show collections;
    //Contar los documentos que están almacenados
    db.incautaciones.aggregate([
        {
            $count:"total_registros"//Crea una clave "total_registros", la cual muestra el total de los documentos registrados en la colección
            }
        ]);
    //Cantidad total incautada
    db.incautaciones.find();
    db.incautaciones.findOne();
    db.incautaciones.aggregate([
        {
            $group:{
                _id:null,
                total_cantidad:{
                    $sum:"$cantidad"
                    }
                }
            }
        ]);

    //Agrupar por año
    db.incautaciones.aggregate([
        {
            $addFields:{
                anio:{$year:'$fecha_hecho'}
                }
            },
        {
            $group:{
                _id:'$anio',
                total:{$sum:'$cantidad'}
                }
            },
        {
            $sort:{
                _id:1
                }
            }
        ]);

    //Agrupación por año y mes
    db.incautaciones.aggregate([
        {
            $group:{
                _id:{
                    anio:{$year:'$fecha_hecho'},
                    mes:{$month:"$fecha_hecho"}
                    },
                total:{
                    $sum:'$cantidad'
                    }
                }
            },
        {
            $sort:{
                "_id.anio":1,"_id.mes":1
                }
            }
        ]);

    //Top 5 Municipios de mayor incautación
    db.incautaciones.aggregate([
        {
            $group:{
                _id:"$cod_muni",
                total:{$sum:'$cantidad'}
                }
            },{
            $sort:{total:-1}
            },{
            $limit:5
            },{
            $lookup:{
                from:"municipios",
                localField:"_id",
                foreignField:"cod_muni",
                as:"muni"
                }
            },{
            $unwind:'$muni'
            },{
            $project:{
                municipio:"$muni.nombre_muni",
                total:1,
                _id:0
                }
            }

        ]);
    //Promedio por municipio --> Top 10 promedios de incautacion
    //por municipio
    db.incautaciones.aggregate(
        [{
            $group:{
                _id: "$cod_muni",
                promedio: {$avg: "$cantidad"}}
            },
            {$lookup:{
                from: "municipios",
                localField: "_id",
                foreignField: "cod_muni",
                as: "municipioSerio"
                } },
            {$unwind: "$municipioSerio"},{
            $project:{
                municipio:'$municipioSerio.nombre_muni',
                total:'$promedio'
                }
            },{
            $sort:{total :-1}
            },
            {
                $limit:10
                }]);

    //Ranking de Departamentos por Incautación
    db.evento.aggregate([
        {
            $lookup:{
                from:'municipios',
                localField:"cod_muni",
                foreignField:"codMuni",
                as:'muni'
                }
            },{
            $unwind:'$muni'
            },
        {
            $lookup:{
                from:'departamentos',
                localField:'muni.codDepto',
                foreignField:'codDepto',
                as:'depto'
                }
            },{
            $unwind:'$depto'
            },{
            $group:{
                _id:{ departamento:'$depto.nomDepto', municipio: "$muni.nomMuni"},
                total:{$sum:'$cantidad'}
                }
            },{
            $sort:{total:-1}
            }
        ]);

    //Ranking de los municipios con
    //más incautaciones que empiezan por "Puerto"
    db.incautaciones.aggregate([
        {
            $lookup:{
                from:'municipios',
                localField:"cod_muni",
                foreignField:"cod_muni",
                as:'muni'
                }
            },
        {$unwind:'$muni'}
        ,{
            $match:{
                'muni.nombre_muni':{$regex:/^Puerto/i}
            }
            },{
            $sort:{total:-1}
            },{
            $group:{
                _id:'$muni.nombre_muni',
                total:{$sum:'$cantidad'}
            }
            }]);

